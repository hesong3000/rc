package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVStreamInfo;
import com.example.demo.po.AVUserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component(value=MCUPublishInitializeTask.taskType)
@Scope("prototype")
public class MCUPublishInitializeTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(MCUPublishInitializeTask.class);
    public final static String taskType = "publish_initialize";
    public final static String taskFailedType = "remove_publisher";    //此过程出现失败，需向MCU发送remove publish
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        log.info("execute MCUPublishInitializeTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        if(client_id == null){
            log.error("{} params invalid, msg: {}",MCUPublishInitializeTask.taskType,msg);
            return;
        }

        //校验客户端是否存活
        String avUserkey = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate, avUserkey);

        if(avUserInfo!=null){
            String client_bindkey = avUserInfo.getBinding_key();
            log.info("mq send response {}: {}", client_bindkey,jsonObject);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindkey, jsonObject);
        }else{
            //客户端下线，removePublish到MCU
            String stream_id = jsonObject.getString("stream_id");
            String mcu_bindkey = jsonObject.getString("mcu_bindkey");
            JSONObject request_msg = new JSONObject();
            request_msg.put("type", MCUPublishInitializeTask.taskFailedType);
            request_msg.put("client_id", client_id);
            request_msg.put("stream_id", stream_id);
            log.info("mq send to mcu {}: {}", mcu_bindkey,request_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, request_msg);

            //删除缓存的媒体流信息
            String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
            RedisUtils.delKey(redisTemplate, avStreamKey);
        }
    }
}
