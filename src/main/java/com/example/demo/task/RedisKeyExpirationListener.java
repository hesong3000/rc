package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.listener.KeyExpirationEventMessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class RedisKeyExpirationListener extends KeyExpirationEventMessageListener{
    private static Logger log = LoggerFactory.getLogger(RedisKeyExpirationListener.class);

    public RedisKeyExpirationListener(RedisMessageListenerContainer listenerContainer) {
        super(listenerContainer);
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void onMessage(Message message, @Nullable byte[] pattern) {
        String expiredKey = message.toString();
        log.info("key: {} is expired now!",expiredKey);
        Map<String, String> map = new HashMap<String, String>();
        if(expiredKey.startsWith(MQConstant.REDIS_USER_KEY_PREFIX)){
            String expiredUserID = expiredKey.substring(MQConstant.REDIS_USER_KEY_PREFIX.length(),expiredKey.length());
            log.info("AVUserInfo expired: {}",expiredUserID);
            map.put("type",RCUserExpiredTask.taskType);
            map.put("client_id",expiredUserID);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map));
        }else if(expiredKey.startsWith(MQConstant.REDIS_MP_KEY_PREFIX)){
            String expiredMPID = expiredKey.substring(MQConstant.REDIS_MP_KEY_PREFIX.length(),expiredKey.length());
            log.info("MPServer expired: {}",expiredMPID);
            map.put("type",RCMPExpiredTask.taskType);
            map.put("mp_id",expiredMPID);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map));
        }
    }


}
