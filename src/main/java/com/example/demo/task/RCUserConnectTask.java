package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVUserInfo;
import com.example.demo.po.DomainRoute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

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
    @Autowired
    DomainDefineBean domainBean;
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

        //判断登录消息是否来自外域
        boolean isFromOutterDomain = false;
        if(jsonObject.containsKey("src_domain")==true){
            //来自外域
            isFromOutterDomain = true;
            String src_domain = jsonObject.getString("src_domain");
            avUserInfo.setSrc_domain(src_domain);
        }else{
            avUserInfo.setSrc_domain(domainBean.getSrcDomain());
        }

        expired = expired*2;    //键的过期间隔为注册间隔的两倍

        boolean ret = RedisUtils.set(redisTemplate,avUserItem, avUserInfo,expired);
        if(ret==false){
            log.error("insert user to redis failed, {}", avUserInfo.toString());
            return;
        }

        //若为本域终端登录，则向外域广播登录消息
        if(isFromOutterDomain==false){
            JSONObject outter_msg = new JSONObject();
            outter_msg.put("type", RCUserConnectTask.taskType);
            outter_msg.put("client_id", client_id);
            outter_msg.put("client_name", client_name);
            outter_msg.put("binding_key", binding_key);
            outter_msg.put("expired",0);
            outter_msg.put("src_domain",domainBean.getSrcDomain());
            List<DomainRoute> new_domain_list = new LinkedList<>();
            DomainRoute domainRoute = domainBean.getBroadcastDomainRoute();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            outter_msg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, outter_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", outter_msg);
        }

        //仅对本域终端的登录回复响应
        if(isFromOutterDomain==false){
            JSONObject response_msg = new JSONObject();
            response_msg.put("type", RCUserConnectTask.taskResType);
            response_msg.put("client_id", client_id);
            response_msg.put("result", "success");
            log.info("mq send to client {}: {}", avUserInfo.getBinding_key(),response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, binding_key, response_msg);
        }
    }
}
