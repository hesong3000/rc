package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

public class RCUserConnectTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCUserConnectTask.class);
    public final static String taskType = "amqp_connect_request";
    private String msg;
    private RedisTemplate<String, Object> redisTemplate;

    public RCUserConnectTask(String msg, RedisTemplate redisTemplate) {
        this.msg = msg;
        this.redisTemplate = redisTemplate;
    }

    @Override
    @Transactional
    public void run() {
        Map<String,Object> map = (Map<String, Object>) JSON.parse(msg);
        String client_id = (String)map.get("client_id");
        String client_name = (String)map.get("client_name");
        String binding_key = (String)map.get("binding_key");
        String avUserItem = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        AVUser avUser = new AVUser();
        avUser.setClient_id(client_id);
        avUser.setClient_name(client_name);
        avUser.setBinding_key(binding_key);
        boolean ret = RedisUtils.set(redisTemplate,avUserItem,avUser,MQConstant.USER_ONLINE_EXPIRE);
        if(ret==false){
            log.error("insert user to redis failed, {}", avUser.toString());
            return;
        }
        log.info("get user connect msg, {}",avUser.toString());
        //AVUser avUser_res = (AVUser)RedisUtils.get(redisTemplate,avUserItem);
    }
}
