package com.example.demo.task;

import com.alibaba.fastjson.JSON;
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

import java.util.*;

@Component(value= MCUMuteStreamTask.taskType)
@Scope("prototype")
public class MCUMuteStreamTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCUMuteStreamTask.class);
    public final static String taskType = "mute_stream_request";
    public final static String taskResType = "mute_stream_response";
    public final static String taskNotType = "stream_muted_notice"; //只有publishstream的mute才会通知给会议的在线成员
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    private int processRequest(JSONObject requestMsg, Result result){
        String client_id = requestMsg.getString("client_id");
        String room_id = requestMsg.getString("room_id");
        String publish_stream_id = requestMsg.getString("stream_id");
        JSONObject options = requestMsg.getJSONObject("options");
        if(client_id==null||room_id==null||publish_stream_id==null||options==null){
            log.error("{}: request msg lack params, msg: {}", MCUMuteStreamTask.taskType, requestMsg);
            return AVErrorType.ERR_PARAM_REQUEST;
        }

        Boolean videoMuted = options.getBoolean("videoMuted");
        if(videoMuted==null)
            videoMuted = false;
        Boolean audioMuted = options.getBoolean("audioMuted");
        if(audioMuted==null)
            audioMuted = false;
        result.client_id = client_id;
        result.publish_stream_id = publish_stream_id;
        result.audioMuted = audioMuted;
        result.videoMuted = videoMuted;
        result.room_id = room_id;

        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(client_id) == false)
            return AVErrorType.ERR_ROOM_KICK;

        //获取在会成员
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo notice_mem = entry.getValue();
            String mem_id = notice_mem.getMem_id();
            boolean mem_online = notice_mem.isMem_Online();
            if(mem_id.compareTo(client_id)!=0 && mem_online==true)
                result.online_mems.add(mem_id);
        }

        //检查是否为publishstream mute操作，若是的话则更新
        PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(publish_stream_id);
        if(publishStreamInfo==null)
            return AVErrorType.ERR_STREAM_SHUTDOWN;

        if(publishStreamInfo.getPublish_clientid().compareTo(client_id)==0){
            result.isPublisher = true;
            publishStreamInfo.setVideoMuted(videoMuted);
            publishStreamInfo.setAudioMuted(audioMuted);

            //更新会议室信息
            if(RedisUtils.hset(redisTemplate, avRoomsKey, avRoomItem, avLogicRoom)==false){
                log.error("redis hset avroominfo failed, hashkey: {}, value: {}", avRoomItem, avLogicRoom);
            }
        }

        //获得处理该发布流的mcu
        String mcu_id = publishStreamInfo.getStream_process_mcuid();
        String mcu_bindkey = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
        //发送处理请求到MCU
        log.info("mq send to mcu {}: {}", mcu_bindkey,requestMsg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, requestMsg);

        return AVErrorType.ERR_NOERROR;
    }

    private int sendResponse(int processCode, Result result){
        if(result.client_id.length()>0){
            JSONObject response_msg = new JSONObject();
            response_msg.put("type", MCUMuteStreamTask.taskResType);
            response_msg.put("retcode", processCode);
            response_msg.put("client_id", result.client_id);
            response_msg.put("stream_id", result.publish_stream_id);
            response_msg.put("room_id", result.room_id);
            JSONObject option_msg = new JSONObject();
            option_msg.put("videoMuted", result.videoMuted);
            option_msg.put("audioMuted", result.audioMuted);
            response_msg.put("options", option_msg);
            String client_sendkey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.client_id;
            log.info("mq send to client {}: {}", client_sendkey,response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_sendkey, response_msg);
        }
        return processCode;
    }

    private int sendNotice(int processCode, Result result){
        if(processCode!=AVErrorType.ERR_NOERROR)
            return processCode;

        if(result.isPublisher==false)
            return processCode;

        JSONObject notice_msg = new JSONObject();
        notice_msg.put("type", MCUMuteStreamTask.taskNotType);
        notice_msg.put("publish_stream_id",result.publish_stream_id);
        notice_msg.put("room_id", result.room_id);
        JSONObject option_msg = new JSONObject();
        option_msg.put("videoMuted", result.videoMuted);
        option_msg.put("audioMuted", result.audioMuted);
        notice_msg.put("options", option_msg);
        Iterator<String> online_memiter = result.online_mems.iterator();
        while(online_memiter.hasNext()){
            String online_memid = online_memiter.next();
            String mem_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+online_memid;
            log.info("mq send notice {}: {}",mem_bindkey,notice_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_bindkey, notice_msg);
        }
        return processCode;
    }

    @Override
    public void run() {
        log.info("execute MCUMuteStreamTask at {}", new Date());
        try {
            JSONObject requestMsg = JSON.parseObject(msg);
            int retcode = AVErrorType.ERR_NOERROR;
            Result result = new Result();
            retcode = processRequest(requestMsg,result);
            retcode = sendResponse(retcode,result);
            sendNotice(retcode,result);
        }catch(Exception e){
            e.printStackTrace();
            return;
        }
    }

    class Result{
        String client_id = "";
        String publish_stream_id = "";
        String room_id = "";
        boolean videoMuted = false;
        boolean audioMuted = false;
        boolean isPublisher = false;
        List<String> online_mems = new LinkedList<>();
    }
}
