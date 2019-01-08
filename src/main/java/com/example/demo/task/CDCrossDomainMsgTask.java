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

@Component(value= CDCrossDomainMsgTask.taskType)
@Scope("prototype")
public class CDCrossDomainMsgTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDCrossDomainMsgTask.class);
    public final static String taskType = "cross_domain_msg";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
         /*
          1、判断用户是否登录在本域，且是否在线
          2、消息解封装
          2、透传消息给用户
         */
        log.info("execute CDCrossDomainMsgTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        String userKey = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate,userKey);
        if(avUserInfo!=null){
            JSONObject encap_msg = requestMsg.getJSONObject("encap_msg");
            log.info("mq send to client {}: {}", avUserInfo.getBinding_key(),encap_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, avUserInfo.getBinding_key(),encap_msg);
        }else{
            log.info("send CDCrossDomainMsgTask ignore, while user: {} is offline", client_id);
        }
    }
}
