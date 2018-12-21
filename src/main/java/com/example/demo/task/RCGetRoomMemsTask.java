package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
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

import java.util.*;

@Component(value=RCGetRoomMemsTask.taskType)
@Scope("prototype")
public class RCGetRoomMemsTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCGetRoomMemsTask.class);
    public final static String taskType = "get_roommems_request";
    public final static String taskResType = "get_roommems_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Override
    public void run() {
        log.info("execute RCGetRoomMemsTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        JSONObject response_msg = new JSONObject();
        String reuqest_client_id = requestMsg.getString("client_id");
        String client_sendkey = MQConstant.MQ_CLIENT_KEY_PREFIX+reuqest_client_id;
        String room_id = requestMsg.getString("room_id");
        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        int retcode = AVErrorType.ERR_NOERROR;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){

            retcode = AVErrorType.ERR_ROOM_NOTEXIST;
        }

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(reuqest_client_id) == false)
            retcode =  AVErrorType.ERR_ROOM_KICK;
        response_msg.put("type", RCGetRoomMemsTask.taskResType);
        response_msg.put("retcode", retcode);
        response_msg.put("room_id", room_id);
        if(retcode!=AVErrorType.ERR_NOERROR){
            log.info("mq send client {}: {}", client_sendkey,response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_sendkey, response_msg);
            return;
        }
        List<Map<String,Object>> mem_list = new ArrayList<>();
        Iterator<Map.Entry<String, RoomMemInfo>> mem_iterator = roomMemInfoMap.entrySet().iterator();
        while (mem_iterator.hasNext()) {
            Map.Entry<String, RoomMemInfo> entry = mem_iterator.next();
            RoomMemInfo roomMemInfo = entry.getValue();
            Map<String,Object> mem_info = new HashMap<>();
            mem_info.put("mem_id", roomMemInfo.getMem_id());
            mem_info.put("mem_name", roomMemInfo.getMem_name());
            mem_list.add(mem_info);
        }
        JSONArray mem_list_array = JSONArray.parseArray(JSONObject.toJSONString(mem_list));
        response_msg.put("mem_list",mem_list_array);
        log.info("mq send client {}: {}", client_sendkey,response_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_sendkey, response_msg);
    }
}
