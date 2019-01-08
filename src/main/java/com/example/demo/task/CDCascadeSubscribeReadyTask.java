package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVStreamInfo;
import com.example.demo.po.DomainRoute;
import com.example.demo.po.MPServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@Component(value= CDCascadeSubscribeReadyTask.taskType)
@Scope("prototype")
public class CDCascadeSubscribeReadyTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDCascadeSubscribeReadyTask.class);
    public final static String taskType = "cascade_subscriber_ready";
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        log.info("execute CDCascadeSubscribeReadyTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String stream_id = requestMsg.getString("stream_id");
        //查找发布流信息
        String avstream_key = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avstream_key);
        if(avStreamInfo==null){
            log.error("{} redis get avstream failed, key: {}", CDCascadeSubscribeReadyTask.taskType, avstream_key);
            return;
        }
        String room_id = avStreamInfo.getRoom_id();
        String room_domain = avStreamInfo.getRoom_domain();
        String mcu_id = requestMsg.getString("mcu_id");
        String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
        MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
        if(mpServerInfo != null){
            //更新MCU使用率信息
            //若publishstream在本域处理，则更新mcu使用率信息
            //流发布成功，占用一路mcu资源
            mpServerInfo.addMcuUseResource(room_id, 1);
            //将MCU的更新信息存储至Redis
            if(RedisUtils.hset(redisTemplate, av_mps_key, av_mp_hashkey, mpServerInfo)==false){
                log.error("redis hset mpserver info failed, key: {} hashket: {}, value: {}",
                        av_mps_key, av_mp_hashkey, mpServerInfo);
            }
        }

        requestMsg.put("room_id", room_id);
        requestMsg.put("room_domain", room_domain);
        //判断room是否在本域
        if(room_domain.compareTo(domainBean.getSrcDomain())==0){
            //暂时先打印结果，之后可能有用
            log.warn("cascade subscriber ready, msg: {}", requestMsg);
        }else{
            //跨域发布到room所在域
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        CDCascadeSubscribeReadyTask.taskType, room_domain, requestMsg);
                return;
            }

            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            requestMsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", requestMsg);
            return;
        }
    }
}
