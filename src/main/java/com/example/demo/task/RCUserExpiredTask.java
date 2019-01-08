package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.DomainRoute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedList;
import java.util.List;

@Component(value=RCUserExpiredTask.taskType)
@Scope("prototype")
public class RCUserExpiredTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCUserExpiredTask.class);
    public final static String taskType = "user_expired_report";
    public final static String procTaskType = "purge_resource_request";
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Autowired
    DomainDefineBean domainBean;

    //直接发送purge_resource_request即可
    @Override
    @Transactional
    public void run() {
        log.info("execute RCUserExpiredTask");
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        JSONObject procMsg = new JSONObject();
        procMsg.put("type", procTaskType);
        procMsg.put("client_id", client_id);
        log.info("mq send to RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,procMsg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, procMsg);

        //判断是否来自本域，若是则向外域广播
        if(procMsg.containsKey("src_domain")==false){
            JSONObject outter_msg = new JSONObject();
            outter_msg.put("type", RCUserExpiredTask.taskType);
            outter_msg.put("client_id", client_id);
            outter_msg.put("src_domain", domainBean.getSrcDomain());
            List<DomainRoute> new_domain_list = new LinkedList<>();
            DomainRoute domainRoute = domainBean.getBroadcastDomainRoute();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            outter_msg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, outter_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", outter_msg);
        }
    }
}
