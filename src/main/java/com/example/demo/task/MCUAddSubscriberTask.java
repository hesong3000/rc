package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVLogicRoom;
import com.example.demo.po.MPServerInfo;
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
import java.util.Map;

@Component(value= MCUAddSubscriberTask.taskType)
@Scope("prototype")
public class MCUAddSubscriberTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCUAddSubscriberTask.class);
    public final static String taskType = "add_subscriber";
    public final static String taskFailType = "msg_fail_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    /*
    1、相关会议室校验，回复失败信息
    2、选择一个MCU（发布流所在的mcu，判定mcu使用率是否满足需求）发送add_subscriberr请求，消息体中存在冗余信息，可以不用改变
    */
    private int processRequest(JSONObject requestMsg, Result result){
        String room_id = requestMsg.getString("room_id");
        String client_id = requestMsg.getString("client_id");
        String stream_id = requestMsg.getString("stream_id");
        String publish_stream_id = requestMsg.getString("publish_stream_id");
        JSONObject jsonOption = requestMsg.getJSONObject("options");
        if(room_id==null||client_id==null||stream_id==null||publish_stream_id==null
                ||jsonOption==null){
            log.error("add_subscriber msg lack params, msg: {}",requestMsg);
            return AVErrorType.ERR_PARAM_REQUEST;
        }
        result.client_id = client_id;
        result.publish_stream_id = publish_stream_id;
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

        //检查会议室中是否发布了此媒体流
        if(avLogicRoom.getPublish_streams().containsKey(publish_stream_id)==false){
            return AVErrorType.ERR_STREAM_SHUTDOWN;
        }

        //获取处理该发布流的MCU信息
        PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(publish_stream_id);
        String mcu_id = publishStreamInfo.getStream_process_mcuid();

        //获取MCU信息
        String mcuinfos_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        String mcuinfo_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
        MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, mcuinfos_key, mcuinfo_hashkey);
        if(mpServerInfo==null)
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;
        else{
            int remain_mcu_res = mpServerInfo.getMax_stream_count()-mpServerInfo.getReserve_stream_count()-mpServerInfo.getUserd_stream_count();
            if(remain_mcu_res<=0)
                return AVErrorType.ERR_MCURES_NOT_ENOUGH;
        }
        //更新MCU信息
        if(RedisUtils.hset(redisTemplate, mcuinfos_key, mcuinfo_hashkey, mpServerInfo)==false){
            log.error("hset mcuinfo failed, hashkey: {}, value: {}", mcuinfo_hashkey,mpServerInfo);
        }

        //向MCU发送addSubscriber请求
        String mcu_bindkey = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
        String client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
        requestMsg.put("client_bindkey", client_bindkey);
        log.info("mq send to mcu {}: {}", mcu_bindkey,requestMsg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, requestMsg);
        return AVErrorType.ERR_NOERROR;
    }

    private int sendFailResponse(int processCode, Result result){
        if(processCode==AVErrorType.ERR_NOERROR)
            return processCode;

        //错误消息回复
        if(result.client_id.length()!=0){
            JSONObject response_msg = new JSONObject();
            response_msg.put("type", MCUAddSubscriberTask.taskFailType);
            response_msg.put("client_id", result.client_id);
            JSONObject failed_msg = new JSONObject();
            failed_msg.put("sub_type", MCUAddSubscriberTask.taskType);
            failed_msg.put("stream_id", result.publish_stream_id);
            failed_msg.put("retcode", processCode);
            response_msg.put("msg",failed_msg);
            String client_bindingkey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.client_id;
            log.info("mq send response {}: {}", client_bindingkey,response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindingkey, response_msg);
        }

        return processCode;
    }

    @Override
    public void run() {
        log.info("execute MCUAddSubscriberTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        int processCode = AVErrorType.ERR_NOERROR;
        Result result = new Result();
        processCode = processRequest(requestMsg,result);
        sendFailResponse(processCode,result);
    }

    class Result{
        String client_id;
        String publish_stream_id;
    }
}
