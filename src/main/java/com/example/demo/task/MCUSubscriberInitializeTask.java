package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVUserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component(value=MCUSubscriberInitializeTask.taskType)
@Scope("prototype")
public class MCUSubscriberInitializeTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCUSubscriberInitializeTask.class);
    public final static String taskType = "subscriber_initialize";
    public final static String taskFailedType = "remove_subscriber";    //此过程出现失败，需向MCU发送remove subscriber
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        log.info("execute MCUSubscriberInitializeTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        String publish_stream_id = jsonObject.getString("publish_stream_id");
        String mcu_bindkey = jsonObject.getString("mcu_bindkey");
        if (client_id == null || publish_stream_id == null || mcu_bindkey == null) {
            log.error("{} params invalid, msg: {}", MCUSubscriberInitializeTask.taskType, msg);
            return;
        }

        //校验客户端是否存活
        String avUserkey = MQConstant.REDIS_USER_KEY_PREFIX + client_id;
        AVUserInfo avUserInfo = (AVUserInfo) RedisUtils.get(redisTemplate, avUserkey);

        if (avUserInfo != null) {
            String client_bindkey = avUserInfo.getBinding_key();
            log.info("mq send response {}: {}", client_bindkey, jsonObject);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindkey, jsonObject);
        } else {
            //客户端下线，removeSubscriber到MCU
            JSONObject removesub_msg = new JSONObject();
            removesub_msg.put("type", MCUSubscriberInitializeTask.taskFailedType);
            removesub_msg.put("client_id", client_id);
            removesub_msg.put("publish_stream_id", publish_stream_id);
            log.info("{} offline mq send to mcu, {}: {}", mcu_bindkey, removesub_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, removesub_msg);
        }
    }
}
