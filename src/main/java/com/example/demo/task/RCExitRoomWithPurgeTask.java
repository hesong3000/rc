package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVLogicRoom;
import com.example.demo.po.PublishStreamInfo;
import com.example.demo.po.RoomMemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;

@Component(value=RCExitRoomWithPurgeTask.taskType)
@Scope("prototype")
public class RCExitRoomWithPurgeTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RCExitRoomWithPurgeTask.class);
    public final static String taskType = "exit_room_with_purge";
    public final static String taskRemovePublisherType = "remove_publisher";
    public final static String taskExitRoomType = "exit_room";

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        log.info("execute RCExitRoomWithPurgeTask");
        JSONObject requestMsg = JSON.parseObject(msg);
        String request_client_id = requestMsg.getString("client_id");
        String request_room_id = requestMsg.getString("room_id");
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

        //遍历该成员在会议室中的发布流，若有则取消发布
        Map<String, PublishStreamInfo> room_publishstreams = avLogicRoom.getPublish_streams();
        Iterator<Map.Entry<String, PublishStreamInfo>> publish_stream_iterator = room_publishstreams.entrySet().iterator();
        while (publish_stream_iterator.hasNext()) {
            Map.Entry<String, PublishStreamInfo> entry = publish_stream_iterator.next();
            PublishStreamInfo streamInfo = entry.getValue();
            if(streamInfo.getPublish_clientid().compareTo(request_client_id)==0){
                JSONObject procMsg = new JSONObject();
                procMsg.put("type",RCExitRoomWithPurgeTask.taskRemovePublisherType);
                procMsg.put("client_id",streamInfo.getPublish_clientid());
                procMsg.put("room_id",avLogicRoom.getRoom_id());
                procMsg.put("stream_id",streamInfo.getPublish_streamid());
                log.info("mq send to RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,procMsg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, procMsg);
            }
        }

        //用户退出会议
        JSONObject exitroom_msg = new JSONObject();
        exitroom_msg.put("type", RCExitRoomWithPurgeTask.taskExitRoomType);
        exitroom_msg.put("client_id", request_client_id);
        exitroom_msg.put("room_id", request_room_id);
        log.info("mq send RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,exitroom_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, exitroom_msg);
    }
}
