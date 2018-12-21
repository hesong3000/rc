package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVRoomInfo;
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

    @Override
    public void run() {
        /*
            遍历用户所在的全部会议室，进行退会操作即可
         */
        log.info("execute RCPurgeResourceTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        String userRoomKey = MQConstant.REDIS_USER_ROOM_KEY_PREFIX+client_id;
        Map<Object, Object> userRoomsMap = RedisUtils.hmget(redisTemplate, userRoomKey);
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
