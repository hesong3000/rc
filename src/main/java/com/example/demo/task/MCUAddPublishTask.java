package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVLogicRoom;
import com.example.demo.po.AVStreamInfo;
import com.example.demo.po.MPServerInfo;
import com.example.demo.po.RoomMemInfo;
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

    /*
    1、相关会议室校验，回复失败信息
    2、将媒体流信息暂时缓存至普通键AV_Stream:[StreamID]中
    3、选择一个MCU发送add_publisher请求，消息体中存在冗余信息，可以不用改变
    */
    private int processRequest(JSONObject requestMsg, Result result){
        String room_id = requestMsg.getString("room_id");
        String client_id = requestMsg.getString("client_id");
        String stream_id = requestMsg.getString("stream_id");
        JSONObject jsonOption = requestMsg.getJSONObject("options");
        if(room_id==null||client_id==null||stream_id==null||jsonOption==null){
            log.error("{} params invalid, msg: {}", MCUAddPublishTask.taskType,msg);
            return AVErrorType.ERR_PARAM_REQUEST;
        }
        Boolean screencast = jsonOption.getBoolean("screencast");
        if(screencast==null)
            screencast = false;
        Boolean audioMuted = jsonOption.getBoolean("audioMuted");
        if(audioMuted==null)
            audioMuted = false;
        Boolean videoMuted = jsonOption.getBoolean("videoMuted");
        if(videoMuted==null)
            videoMuted = false;
        result.client_id = client_id;
        result.stream_id = stream_id;
        String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;

        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(client_id) == false)
            return AVErrorType.ERR_ROOM_KICK;

        //校验发布媒体流是否冲突
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avStreamKey);
        if(avStreamInfo==null){
            avStreamInfo = new AVStreamInfo();
            avStreamInfo.setRoom_id(room_id);
            avStreamInfo.setStream_id(stream_id);
            avStreamInfo.setScreencast(screencast);
            avStreamInfo.setVideoMuted(videoMuted);
            avStreamInfo.setAudioMuted(audioMuted);
            avStreamInfo.setPublisher_id(client_id);
            avStreamInfo.setPublish(true);
        }else{
            return AVErrorType.ERR_STREAM_CONFLICT;
        }

        if(!RedisUtils.set(redisTemplate,avStreamKey,avStreamInfo)){
            log.error("redis set failed, key: {}, value: {}", avStreamKey, avStreamInfo);
            return AVErrorType.ERR_REDIS_STORE;
        }

        //选择一个合适的MCU处理AddPublish请求
        /*1、遍历hash键AV_MPs的所有项，检查stream_list中的room_id是否匹配请求中的room_id，如果存在的话则返回
             此MP，原因是尽量让同一会议室的所有流由同一个MP处理，如果所选的MP处理路上达到上限，则暂时作拒绝处理（后续再考虑级联情况）
          2、若没有匹配的room_id，则选择空闲率最高的MP作媒体处理
        */
        String desired_mp_bindingkey = "";
        int has_idle_stream_count = 0;
        String avMPs_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        Map<Object,Object> av_mps = RedisUtils.hmget(redisTemplate,avMPs_key);
        Iterator<Map.Entry<Object, Object>> iterator = av_mps.entrySet().iterator();
        while (iterator.hasNext()){
            MPServerInfo mpServerInfo = (MPServerInfo)iterator.next().getValue();
            int mp_idle_stream_count = mpServerInfo.getMax_stream_count()-mpServerInfo.getReserve_stream_count()-mpServerInfo.getUserd_stream_count();
            String mp_bindingkey = mpServerInfo.getBinding_key();
            if(mpServerInfo.getRoom_list().containsKey(room_id)){
                desired_mp_bindingkey = mp_bindingkey;
                has_idle_stream_count = mp_idle_stream_count;
                break;
            }else{
                if(mp_idle_stream_count>has_idle_stream_count){
                    has_idle_stream_count = mp_idle_stream_count;
                    desired_mp_bindingkey = mp_bindingkey;
                }
            }
        }

        if(desired_mp_bindingkey.length()==0||has_idle_stream_count==0)
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;

        //将AddPublish请求发送至选定的MCU
        String client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
        requestMsg.put("client_bindkey", client_bindkey);
        log.info("mq send to mcu {}: {}", desired_mp_bindingkey,requestMsg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, desired_mp_bindingkey, requestMsg);
        return AVErrorType.ERR_NOERROR;
    }

    //返回在此阶段就能校验出来的错误，返回至请求者
    private int sendFailResponse(int processCode, Result result){
        if(processCode==AVErrorType.ERR_NOERROR)
            return processCode;

        //错误消息回复
        if(result.client_id.length()!=0){
            JSONObject response_msg = new JSONObject();
            response_msg.put("type", MCUAddPublishTask.taskFailType);
            response_msg.put("client_id", result.client_id);
            JSONObject failed_msg = new JSONObject();
            failed_msg.put("sub_type", MCUAddPublishTask.taskType);
            failed_msg.put("stream_id", result.stream_id);
            failed_msg.put("retcode", processCode);
            response_msg.put("msg",failed_msg);
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
