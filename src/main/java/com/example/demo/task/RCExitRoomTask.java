package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVLogicRoom;
import com.example.demo.po.RoomMemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Component(value=RCExitRoomTask.taskType)
@Scope("prototype")
public class RCExitRoomTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RCExitRoomTask.class);
    public final static String taskType = "exit_room";
    public final static String taskNotType = "room_memberout_notice";
    public final static String deleteTaskType = "delete_room";

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        log.info("execute RCExitRoomTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String request_room_id = requestMsg.getString("room_id");
        String request_client_id = requestMsg.getString("client_id");
        if(request_room_id == null || request_client_id == null){
            log.error("{}: request msg invalid, msg: {}", RCExitRoomTask.taskType, requestMsg);
            return;
        }

        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+request_room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            log.error("{}: failed, room not exist msg: {}", RCExitRoomTask.taskType, requestMsg);
            return;
        }

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(request_client_id) == false){
            log.error("{}: failed, member has kickout msg: {}", RCExitRoomTask.taskType, requestMsg);
            return;
        }

        //检查该成员是否已经退出会议室
        RoomMemInfo curRoomMemInfo = avLogicRoom.getRoom_mems().get(request_client_id);
        if(curRoomMemInfo.isMem_Online()==false)
            return;

        //广播退出通知
        boolean isAllMemExit = true;
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo roomMemInfo = entry.getValue();
            String mem_id = roomMemInfo.getMem_id();
            boolean mem_online = roomMemInfo.isMem_Online();
            if(mem_id.compareTo(request_client_id) ==0 || mem_online == false)
                continue;
            if(mem_online == true)
                isAllMemExit = false;
            String mem_routingkey = MQConstant.MQ_CLIENT_KEY_PREFIX+mem_id;
            Map<String, String> map_res = new HashMap<String, String>();
            map_res.put("type", RCExitRoomTask.taskNotType);
            map_res.put("room_id", avLogicRoom.getRoom_id());
            map_res.put("client_id", request_client_id);
            log.info("mq send notice {}: {}",mem_routingkey,JSON.toJSON(map_res));
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_routingkey, JSON.toJSON(map_res));
        }

        //更新会议室成员状态，并重新存入redis
        avLogicRoom.getRoom_mems().get(request_client_id).setMem_Online(false);
        RedisUtils.hdel(redisTemplate,avRoomsKey,avRoomItem);
        if(!RedisUtils.hset(redisTemplate,avRoomsKey,avRoomItem, avLogicRoom)){
            log.error("{}: redis hset avroom failed! {}",  RCExitRoomTask.taskType, avLogicRoom.toString());
            return;
        }

        //如果会议室只有两人,且都已退出,则自动发送删除会议室命令
        if(isAllMemExit==true && avLogicRoom.getRoom_mems().size()==2){
            JSONObject delete_room_msg = new JSONObject();
            delete_room_msg.put("type", RCExitRoomTask.deleteTaskType);
            delete_room_msg.put("client_id", avLogicRoom.getCreator_id());
            delete_room_msg.put("room_id", avLogicRoom.getRoom_id());
            log.info("mq send RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,delete_room_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, delete_room_msg);
        }
    }
}
