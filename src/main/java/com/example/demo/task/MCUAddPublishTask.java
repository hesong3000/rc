package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

@Component(value= MCUAddPublishTask.taskType)
@Scope("prototype")
public class MCUAddPublishTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(MCUAddPublishTask.class);
    public final static String taskType = "add_publisher";
    public final static String taskFailType = "msg_fail_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    /*
    1、相关会议室校验，回复失败信息
    2、将媒体流信息暂时缓存至普通键AV_Stream:[StreamID]中
    3、选择一个MCU发送add_publisher请求，消息体中存在冗余信息，可以不用改变
    */

    /*
        跨域级联增添
        1、发布流只在本域发布即可
        2、若room所在域不是本域，则需要在本域对应room域的emcu处理该请求
        3、若本rc没有对应room域的域路由或找不到对应的emcu，则直接报错
     */
    private int processRequest(JSONObject requestMsg, Result result){
        String room_id = requestMsg.getString("room_id");
        String client_id = requestMsg.getString("client_id");
        String stream_id = requestMsg.getString("stream_id");
        String room_domain = requestMsg.getString("room_domain");
        JSONObject jsonOption = requestMsg.getJSONObject("options");
        if(room_id==null||client_id==null||stream_id==null||jsonOption==null||room_domain==null){
            log.error("{} params invalid, msg: {}", MCUAddPublishTask.taskType,msg);
            return AVErrorType.ERR_PARAM_REQUEST;
        }
        result.client_id = client_id;
        result.stream_id = stream_id;

        Boolean screencast = jsonOption.getBoolean("screencast");
        if(screencast==null)
            screencast = false;
        Boolean audioMuted = jsonOption.getBoolean("audioMuted");
        if(audioMuted==null)
            audioMuted = false;
        Boolean videoMuted = jsonOption.getBoolean("videoMuted");
        if(videoMuted==null)
            videoMuted = false;

        String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
        //校验发布媒体流是否冲突
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avStreamKey);
        if(avStreamInfo==null){
            avStreamInfo = new AVStreamInfo();
            avStreamInfo.setRoom_id(room_id);
            avStreamInfo.setRoom_domain(room_domain);
            avStreamInfo.setStream_id(stream_id);
            avStreamInfo.setScreencast(screencast);
            avStreamInfo.setVideoMuted(videoMuted);
            avStreamInfo.setAudioMuted(audioMuted);
            avStreamInfo.setPublisher_id(client_id);
            avStreamInfo.setPublish(true);
        }else{
            return AVErrorType.ERR_STREAM_CONFLICT;
        }

        //判断mcu资源情况
        String desired_mp_bindingkey = "";
        int has_idle_stream_count = 0;
        String avMPs_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        Map<Object,Object> av_mps = RedisUtils.hmget(redisTemplate,avMPs_key);
        Iterator<Map.Entry<Object, Object>> mcu_iter = av_mps.entrySet().iterator();

        String selectMcu_id = "";
        //判断会议室所在域是否为本域，此处需要分别处理
        if(domainBean.getSrcDomain().compareTo(room_domain)!=0){
            //若会议室不在本域，则需要选择合适的emcu处理addPublisher请求
            //判定该域是否配置通向room域的domainExchange服务，使得本域rc可与room域的rc进行信令通信
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute==null){
                return AVErrorType.ERR_ROUTE_NOTEXIST;
            }

            //判定是否具备通向room域的emcu，且资源足够
            while (mcu_iter.hasNext()){
                MPServerInfo mpServerInfo = (MPServerInfo)mcu_iter.next().getValue();
                int mp_idle_stream_count = mpServerInfo.getMcuIdleReource();
                String mp_bindingkey = mpServerInfo.getBinding_key();
                boolean isEmcu = mpServerInfo.isEmcu();
                String emcu_avaliable_domain = mpServerInfo.getAval_domain();
                if(isEmcu==false || emcu_avaliable_domain.compareTo(room_domain)!=0)
                    continue;
                if(mp_idle_stream_count>has_idle_stream_count){
                    has_idle_stream_count = mp_idle_stream_count;
                    desired_mp_bindingkey = mp_bindingkey;
                    selectMcu_id = mpServerInfo.getMp_id();
                }
            }
        }else{
            //若room域在本域，选择一个合适的MCU处理AddPublish请求(1判断取消，不利于mcu的负载均衡)
            /*1、遍历hash键AV_MPs的所有项，检查stream_list中的room_id是否匹配请求中的room_id，如果存在的话则返回
                 此MP，原因是尽量让同一会议室的所有流由同一个MP处理，如果所选的MP处理路上达到上限，则暂时作拒绝处理（后续再考虑级联情况）
              2、若没有匹配的room_id，则选择空闲率最高的MP作媒体处理
            */
            while (mcu_iter.hasNext()){
                MPServerInfo mpServerInfo = (MPServerInfo)mcu_iter.next().getValue();
                int mp_idle_stream_count = mpServerInfo.getMcuIdleReource();
                String mp_bindingkey = mpServerInfo.getBinding_key();
                boolean isEmcu = mpServerInfo.isEmcu();
                if(isEmcu==true)
                    continue;
                if(mp_idle_stream_count>has_idle_stream_count){
                    has_idle_stream_count = mp_idle_stream_count;
                    desired_mp_bindingkey = mp_bindingkey;
                    selectMcu_id = mpServerInfo.getMp_id();
                }
            }
        }

        //判断资源是否不足
        if(desired_mp_bindingkey.length()==0||has_idle_stream_count==0||selectMcu_id.length()==0)
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;

        //更新mcu信息，此处只为room占位，不占用资源
        String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+selectMcu_id;
        MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
        if(mpServerInfo == null){
            log.error("can not get MPServerInfo, msg: {}", msg);
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;
        }
        mpServerInfo.addMcuUseResource(room_id, stream_id,0);
        //将MCU的更新信息存储至Redis
        if(RedisUtils.hset(redisTemplate, av_mps_key, av_mp_hashkey, mpServerInfo)==false){
            log.error("redis hset mpserver info failed, key: {} hashket: {}, value: {}",
                    av_mps_key, av_mp_hashkey, mpServerInfo);
        }

        //仅在发布流的域的redis中增加发布流信息
        if(!RedisUtils.set(redisTemplate,avStreamKey,avStreamInfo)){
            log.error("redis set failed, key: {}, value: {}", avStreamKey, avStreamInfo);
            return AVErrorType.ERR_REDIS_STORE;
        }

        //将AddPublish请求发送至选定的MCU
        String client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
        //client_bindkey字段标识谁来接收处理接下来的offer、answer、candidate消息，在发布流端直接为终端即可
        requestMsg.put("client_bindkey", client_bindkey);
        log.info("mq send to mcu {}: {}", desired_mp_bindingkey,requestMsg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, desired_mp_bindingkey, requestMsg);
        return AVErrorType.ERR_NOERROR;
    }

    //返回在此阶段就能校验出来的错误，返回至请求者(addPublish请求只发生在本域，若出现错误直接发送至终端即可)
    private int sendFailResponse(int processCode, Result result){
        if(processCode==AVErrorType.ERR_NOERROR)
            return processCode;

        //错误消息回复
        if(result.client_id.length()!=0){
            JSONObject response_msg = new JSONObject();
            response_msg.put("type", MCUAddPublishTask.taskFailType);
            response_msg.put("client_id", result.client_id);
            response_msg.put("stream_id", result.stream_id);
            response_msg.put("is_publisher", true);
            response_msg.put("retcode", processCode);
            String client_bindingkey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.client_id;
            log.info("mq send response {}: {}", client_bindingkey,response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindingkey, response_msg);
        }
        return processCode;
    }

    @Override
    @Transactional
    public void run() {
        log.info("execute MCUAddPublishTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        int processCode = AVErrorType.ERR_NOERROR;
        Result result = new Result();
        processCode = processRequest(requestMsg,result);
        sendFailResponse(processCode,result);
    }

    class Result{
        String client_id = "";
        String stream_id = "";
    }
}
