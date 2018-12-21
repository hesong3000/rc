package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.po.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

@Component(value= MCUPublishReadyTask.taskType)
@Scope("prototype")
public class MCUPublishReadyTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(MCUPublishReadyTask.class);
    public final static String taskType = "publish_ready";
    public final static String taskNotType = "stream_add_notice";
    public final static String taskFailType = "remove_publisher";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        /*
            1、检测客户端是否存活，若存活则发送publish_ready到客户端，若不存活则发送removePublish请求到MCU，逻辑退出
            2、更新AV_MPs哈希键和AV_Rooms哈希键，必须正确，错误打印ERROR级别日志，为逻辑错误，需要检查原因
            3、向会议室中再会的各成员发送stream_add_notice通知
         */
        log.info("execute MCUPublishReadyTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        String stream_id = jsonObject.getString("stream_id");
        String mcu_id = jsonObject.getString("mcu_id");
        String avUserKey = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        //检查客户端是否存活
        AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate,avUserKey);
        if(avUserInfo==null){
            //发送remove_publisher到处理该媒体流的MCU
            JSONObject fail_rollback_msg = new JSONObject();
            fail_rollback_msg.put("type", MCUPublishReadyTask.taskFailType);
            fail_rollback_msg.put("client_id", client_id);
            fail_rollback_msg.put("stream_id", stream_id);
            String mcu_bindkey =MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
            log.warn("mq send rollback_msg to mcu {}: {}", mcu_bindkey,fail_rollback_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey,fail_rollback_msg);
            return;
        }

        //透传publish_ready消息到客户端
        String client_bindkey = avUserInfo.getBinding_key();
        log.warn("mq send msg to client  {}: {}", client_bindkey,jsonObject);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindkey,jsonObject);

        //检查AV_Stream:[StreamID]键，获取stream所在会议室以及媒体流属性信息，用于更新会议室
        String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avStreamKey);
        if(avStreamInfo==null){
            //此处获取不到则属于逻辑错误，检查BUG
            log.error("can not get streaminfo, msg: {}", msg);
            return;
        }

        String roomID = avStreamInfo.getRoom_id();
        //获取逻辑会议室，不存在则为逻辑错误
        String avRooms_key = MQConstant.REDIS_AVROOMS_KEY;
        String avRoom_hashKey = MQConstant.REDIS_ROOM_KEY_PREFIX+roomID;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRooms_key, avRoom_hashKey);
        if(avLogicRoom == null){
            //此处获取不到则属于逻辑错误，检查BUG
            log.error("can not get logicroom, msg: {}", msg);
            return;
        }

        //更新逻辑会议室信息-room_mems
        RoomMemInfo roomMemInfo = avLogicRoom.getRoom_mems().get(client_id);
        if(roomMemInfo!=null){
            roomMemInfo.getPublish_streams().put(stream_id,stream_id);
        }else{
            //此处获取不到则属于逻辑错误，检查BUG
            log.error("can not get roomMemInfo, msg: {}", msg);
            return;
        }
        //更新逻辑会议室信息-publish_streams
        PublishStreamInfo publishStreamInfo = new PublishStreamInfo();
        publishStreamInfo.setPublish_clientid(client_id);
        publishStreamInfo.setPublish_streamid(stream_id);
        publishStreamInfo.setScreencast(avStreamInfo.getScreencast());
        publishStreamInfo.setStream_process_mcuid(mcu_id);
        publishStreamInfo.setAudioMuted(avStreamInfo.getAudioMuted());
        publishStreamInfo.setVideoMuted(avStreamInfo.getVideoMuted());
        avLogicRoom.getPublish_streams().put(stream_id, publishStreamInfo);

        //向再会的用户发出stream_add_notice通知
        JSONObject notice_msg = new JSONObject();
        notice_msg.put("type", MCUPublishReadyTask.taskNotType);
        notice_msg.put("room_id", roomID);
        notice_msg.put("client_id", client_id);
        notice_msg.put("stream_id", stream_id);
        JSONObject option_msg = new JSONObject();
        option_msg.put("video", !publishStreamInfo.isVideoMuted());
        option_msg.put("audio", !publishStreamInfo.isAudioMuted());
        option_msg.put("screencast",publishStreamInfo.isScreencast());
        notice_msg.put("options",option_msg);
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo notice_mem = entry.getValue();
            String mem_id = notice_mem.getMem_id();
            boolean mem_online = notice_mem.isMem_Online();
            if(mem_id.compareTo(client_id)==0 || mem_online == false)
                continue;
            String mem_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+mem_id;

            log.info("mq send notice {}: {}",mem_bindkey,notice_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_bindkey, notice_msg);
        }

        //将更新后的逻辑会议室信息存储
        if(RedisUtils.hset(redisTemplate, avRooms_key, avRoom_hashKey, avLogicRoom)==false){
            log.error("redis hset avroominfo failed, key: {} hashket: {}, value: {}",
                    avRooms_key, avRoom_hashKey, avLogicRoom);
            return;
        }

        //更新MCU使用率信息
        String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
        MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
        if(mpServerInfo!=null){
            mpServerInfo.setUserd_stream_count(mpServerInfo.getUserd_stream_count()+1);
            if(mpServerInfo.getRoom_list().containsKey(roomID)==true){
                mpServerInfo.getRoom_list().put(roomID, mpServerInfo.getRoom_list().get(roomID)+1);
            }else{
                mpServerInfo.getRoom_list().put(roomID, 1);
            }
        }else{
            log.error("can not get MPServerInfo, msg: {}", msg);
            return;
        }

        //将MCU的更新信息存储至Redis
        if(RedisUtils.hset(redisTemplate, av_mps_key, av_mp_hashkey, mpServerInfo)==false){
            log.error("redis hset mpserver info failed, key: {} hashket: {}, value: {}",
                    av_mps_key, av_mp_hashkey, mpServerInfo);
            return;
        }
    }
}
