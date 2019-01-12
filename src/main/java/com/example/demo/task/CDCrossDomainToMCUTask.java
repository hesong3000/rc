package com.example.demo.task;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.MQConstant;
import com.example.demo.po.MPServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component(value= CDCrossDomainToMCUTask.taskType)
@Scope("prototype")
public class CDCrossDomainToMCUTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDCrossDomainMsgTask.class);
    public final static String taskType = "crossdomain_to_mcu";
    public final static String addSubscribeType = "add_subscriber";
    public final static String removeSubscriberType = "remove_subscriber";
    public final static String removePublisherType = "remove_publisher";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        log.info("execute CDCrossDomainToMCUTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String mcu_id = requestMsg.getString("mcu_id");
        JSONObject encap_msg = requestMsg.getJSONObject("encap_msg");
        String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
        String mcu_msg_type = encap_msg.getString("type");
        MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
        if(mpServerInfo == null){
            log.warn("{} failed while can not get MPServerInfo, msg: {}", CDCrossDomainToMCUTask.taskType, msg);
            return;
        }

        String mcu_bindkey = mpServerInfo.getBinding_key();
        if(mcu_msg_type.compareTo(CDCrossDomainToMCUTask.addSubscribeType)==0){
            if(mpServerInfo.getMcuIdleReource()<=0){
                log.warn("{} failed while not enough resource",CDCrossDomainToMCUTask.addSubscribeType);
                return;
            }
        }else if(mcu_msg_type.compareTo(CDCrossDomainToMCUTask.removePublisherType)==0){
            //删除AVStreamInfo键
            String stream_id = encap_msg.getString("stream_id");
            RedisUtils.delKey(redisTemplate, MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id);
            String room_id = encap_msg.getString("room_id");
            //更新mcu资源
            mpServerInfo.clearMcuUseResourceOnRemove(room_id,stream_id);
            //将MCU的更新信息存储至Redis
            if(RedisUtils.hset(redisTemplate, av_mps_key, av_mp_hashkey, mpServerInfo)==false){
                log.error("redis hset mpserver info failed, key: {} hashket: {}, value: {}",
                        av_mps_key, av_mp_hashkey, mpServerInfo);
            }
        }else if(mcu_msg_type.compareTo(CDCrossDomainToMCUTask.removeSubscriberType)==0){
            String room_id = encap_msg.getString("room_id");
            String publish_stream_id = encap_msg.getString("publish_stream_id");
            //更新mcu资源
            mpServerInfo.releaseMcuUseResource(room_id,publish_stream_id,1);
            //将MCU的更新信息存储至Redis
            if(RedisUtils.hset(redisTemplate, av_mps_key, av_mp_hashkey, mpServerInfo)==false){
                log.error("redis hset mpserver info failed, key: {} hashket: {}, value: {}",
                        av_mps_key, av_mp_hashkey, mpServerInfo);
            }
        }

        log.info("mq send to mcu {}: {}", mcu_bindkey,encap_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, encap_msg);
    }
}
