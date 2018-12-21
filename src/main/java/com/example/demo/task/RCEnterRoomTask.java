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
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Component(value=RCEnterRoomTask.taskType)
@Scope("prototype")
public class RCEnterRoomTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCEnterRoomTask.class);
    public final static String taskType = "enter_room";
    public final static String taskResType = "room_enter_reponse";
    public final static String taskNotType = "room_memberin_notice";

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    /*
        1、校验会议室是否存在，会议室是否有该成员
        2、更新会议室中的成员online状态
        3、发送room_enter_reponse到请求成员
        4、发送room_memberin_notice通知到会议室其他上线成员
     */
    private int processRequest(JSONObject requestMsg, Result result){
        String request_room_id = requestMsg.getString("room_id");
        String request_client_id = requestMsg.getString("client_id");
        if(request_room_id == null || request_client_id == null)
            return AVErrorType.ERR_PARAM_REQUEST;
        result.request_client_id = request_client_id;
        result.request_room_id = request_room_id;
        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+request_room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }
        result.avLogicRoom = avLogicRoom;

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(request_client_id) == false)
            return AVErrorType.ERR_ROOM_KICK;

        //更新会议室成员状态，并重新存入redis
        avLogicRoom.getRoom_mems().get(request_client_id).setMem_Online(true);
        if(!RedisUtils.hset(redisTemplate,avRoomsKey,avRoomItem, avLogicRoom)){
            log.error("redis hset avroom failed! {}", avLogicRoom.toString());
            return AVErrorType.ERR_REDIS_STORE;
        }

        return AVErrorType.ERR_NOERROR;
    }

    private int sendResponse(int processCode, Result result){
        JSONObject responseMsg = new JSONObject();
        responseMsg.put("type",RCEnterRoomTask.taskResType);
        if(processCode == AVErrorType.ERR_NOERROR){
            AVLogicRoom avLogicRoom = result.avLogicRoom;
            responseMsg.put("retcode",processCode);
            responseMsg.put("room_id", avLogicRoom.getRoom_id());
            responseMsg.put("room_name", avLogicRoom.getRoom_name());
            responseMsg.put("creator_id", avLogicRoom.getCreator_id());
            responseMsg.put("create_time", avLogicRoom.getCreate_time().getTime());
            List<Map<String,Object>> mem_list = new ArrayList<>();
            Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, RoomMemInfo> entry = iterator.next();
                RoomMemInfo roomMemInfo = entry.getValue();
                Map<String,Object> mem_info = new HashMap<>();
                mem_info.put("mem_id", roomMemInfo.getMem_id());
                mem_info.put("mem_name", roomMemInfo.getMem_name());
                mem_info.put("mem_online", roomMemInfo.isMem_Online());
                mem_list.add(mem_info);
            }
            JSONArray mem_list_array = JSONArray.parseArray(JSONObject.toJSONString(mem_list));
            responseMsg.put("mem_list",mem_list_array);
        }else{
            responseMsg.put("retcode",processCode);
            responseMsg.put("room_id",result.request_room_id);
        }

        if(result.request_client_id.length()!=0){
            String send_routekey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.request_client_id;
            log.info("mq send response {}: {}",send_routekey,responseMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, send_routekey, responseMsg);
        }

        return processCode;
    }

    private int sendNotice(int processCode, Result result){
        if(processCode != AVErrorType.ERR_NOERROR)
            return -1;
        AVLogicRoom avLogicRoom = result.avLogicRoom;
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo roomMemInfo = entry.getValue();
            String mem_id = roomMemInfo.getMem_id();
            boolean mem_online = roomMemInfo.isMem_Online();
            if(mem_id.compareTo(result.request_client_id)==0 || mem_online == false)
                continue;
            String mem_routingkey = MQConstant.MQ_CLIENT_KEY_PREFIX+mem_id;
            Map<String, String> map_res = new HashMap<String, String>();
            map_res.put("type", RCEnterRoomTask.taskNotType);
            map_res.put("room_id", avLogicRoom.getRoom_id());
            map_res.put("client_id", result.request_client_id);
            log.info("mq send notice {}: {}",mem_routingkey,JSON.toJSON(map_res));
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_routingkey, JSON.toJSON(map_res));
        }

        return processCode;
    }

    @Override
    @Transactional
    public void run() {
        log.info("execute RCEnterRoomTask at {}", new Date());
        try{
            JSONObject requestMsg = JSON.parseObject(msg);
            int processCode = AVErrorType.ERR_NOERROR;
            Result result = new Result();
            processCode = processRequest(requestMsg,result);
            processCode = sendResponse(processCode,result);
            sendNotice(processCode,result);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    class Result{
        String request_room_id = "";
        String request_client_id = "";
        AVLogicRoom avLogicRoom;
    }
}
