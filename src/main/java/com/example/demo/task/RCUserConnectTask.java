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
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component(value=RCUserConnectTask.taskType)
@Scope("prototype")
public class RCUserConnectTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCUserConnectTask.class);
    public final static String taskType = "amqp_connect_request";
    public final static String taskResType = "amqp_connect_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    @Transactional
    public void run() {
        log.info("execute RCUserConnectTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        String client_name = jsonObject.getString("client_name");
        String binding_key = jsonObject.getString("binding_key");
        Integer expired = jsonObject.getInteger("expired");
        if(client_id==null || client_name==null || binding_key==null||expired==null){
            log.error("amqp_connect_request failed, msg: {}",msg);
            return;
        }

        String avUserItem = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        AVUserInfo avUserInfo = new AVUserInfo();
        avUserInfo.setClient_id(client_id);
        avUserInfo.setClient_name(client_name);
        avUserInfo.setBinding_key(binding_key);

        expired = expired*2;    //键的过期间隔为注册间隔的两倍
        boolean ret = RedisUtils.set(redisTemplate,avUserItem, avUserInfo,expired);
        if(ret==false){
            log.error("insert user to redis failed, {}", avUserInfo.toString());
            return;
        }
        AVUserInfo avUser_Info_res = (AVUserInfo)RedisUtils.get(redisTemplate,avUserItem);
        if(avUser_Info_res !=null){
            Map<String, String> map_res = new HashMap<String, String>();
            map_res.put("type", RCUserConnectTask.taskResType);
            map_res.put("client_id", avUser_Info_res.getClient_id());
            map_res.put("result","success");
            log.info("mq send response {}: {}", avUserInfo.getBinding_key(),JSON.toJSONString(map_res));
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, avUserInfo.getBinding_key(), JSON.toJSON(map_res));
        }
    }
}
