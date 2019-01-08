package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVRoomInfo;
import com.example.demo.po.DomainRoute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component(value=RCPurgeResourceTask.taskType)
@Scope("prototype")
public class RCPurgeResourceTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCPurgeResourceTask.class);
    public final static String taskType = "purge_resource_request";
    public final static String PurgetaskType = "exit_room_with_purge";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        /*
            遍历用户所在的全部会议室，进行退会操作即可，进行广播
         */
        log.info("execute RCPurgeResourceTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        boolean is_inner_msg = false;
        if(requestMsg.containsKey("src_domain")==false)
            is_inner_msg = true;

        if(is_inner_msg == true){
            //广播此消息
            requestMsg.put("src_domain",domainBean.getSrcDomain());
            List<DomainRoute> new_domain_list = new LinkedList<>();
            DomainRoute domainRoute = domainBean.getBroadcastDomainRoute();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            requestMsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", requestMsg);
        }

        String userRoomKey = MQConstant.REDIS_USER_ROOM_KEY_PREFIX+client_id;
        Map<Object, Object> userRoomsMap = RedisUtils.hmget(redisTemplate, userRoomKey);
        if(userRoomsMap==null)
            return;

        Iterator<Map.Entry<Object, Object>> userRoom_iterator = userRoomsMap.entrySet().iterator();
        while(userRoom_iterator.hasNext()){
            AVRoomInfo avRoomInfo = (AVRoomInfo)userRoom_iterator.next().getValue();
            JSONObject purge_msg = new JSONObject();
            purge_msg.put("type", RCPurgeResourceTask.PurgetaskType);
            purge_msg.put("client_id", client_id);
            purge_msg.put("room_id", avRoomInfo.getRoom_id());
            log.info("mq send RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,purge_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, purge_msg);
        }
    }
}
