package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
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

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Component(value= GetRoomStreamsRequest.taskType)
@Scope("prototype")
public class GetRoomStreamsRequest extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(GetRoomStreamsRequest.class);
    public final static String taskType = "get_room_streams_request";
    public final static String taskResback = "get_room_streams_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    /*
        1、校验get_room_streams_request信令参数，会议是否存在，会议成员是否存在等
        2、获取逻辑会议室中的发布流信息，发送响应消息至客户端
    */
    private int processRequest(JSONObject requestMsg, Result result){
        String request_room_id = requestMsg.getString("room_id");
        String request_client_id = requestMsg.getString("client_id");
        if(request_room_id == null || request_client_id == null)
            return AVErrorType.ERR_PARAM_REQUEST;
        result.client_id = request_client_id;
        result.room_id = request_room_id;
        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+request_room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }
        result.publish_streams = avLogicRoom.getPublish_streams();

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(request_client_id) == false)
            return AVErrorType.ERR_ROOM_KICK;

        return AVErrorType.ERR_NOERROR;
    }

    private int sendResponse(int processCode, Result result){
        JSONObject responseMsg = new JSONObject();
        responseMsg.put("type", GetRoomStreamsRequest.taskResback);
        responseMsg.put("client_id", result.client_id);
        responseMsg.put("room_id", result.room_id);
        if(processCode == AVErrorType.ERR_NOERROR) {
            responseMsg.put("retcode", processCode);
            JSONArray stream_array = new JSONArray();
            Iterator<Map.Entry<String, PublishStreamInfo>> iterator = result.publish_streams.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, PublishStreamInfo> entry = iterator.next();
                PublishStreamInfo publishStreamInfo = entry.getValue();
                JSONObject publishstream_obj = new JSONObject();
                publishstream_obj.put("publish_streamid", publishStreamInfo.getPublish_streamid());
                publishstream_obj.put("publish_clientid", publishStreamInfo.getPublish_clientid());
                publishstream_obj.put("screencast", publishStreamInfo.isScreencast());
                publishstream_obj.put("audioMuted", publishStreamInfo.isAudioMuted());
                publishstream_obj.put("videoMuted", publishStreamInfo.isVideoMuted());
                stream_array.add(publishstream_obj);
            }
            responseMsg.put("streams", stream_array);
        }else{
            responseMsg.put("retcode", processCode);
        }

        if(result.client_id.length()>0){
            String send_routekey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.client_id;
            log.info("mq send response {}: {}",send_routekey,responseMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, send_routekey, responseMsg);
        }

        return processCode;
    }

    @Override
    public void run() {
        log.info("execute GetRoomStreamsRequest at {}", new Date());
        try{
            JSONObject requestMsg = JSON.parseObject(msg);
            int processCode = AVErrorType.ERR_NOERROR;
            Result result = new Result();
            processCode = processRequest(requestMsg,result);
            sendResponse(processCode,result);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    class Result{
        String client_id = "";
        String room_id = "";
        Map<String, PublishStreamInfo> publish_streams = new HashMap<>();
    }
}
