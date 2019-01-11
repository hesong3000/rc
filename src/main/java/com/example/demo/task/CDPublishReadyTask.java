package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component(value= CDPublishReadyTask.taskType)
@Scope("prototype")
public class CDPublishReadyTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDPublishReadyTask.class);
    public final static String taskType = "crossdomain_publish_ready";
    public final static String taskNotType = "stream_add_notice";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;
    @Override
    public void run() {
        /*
            跨域发送crossdomain_publish_ready
         */
        log.info("execute CDPublishReadyTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        String stream_id = jsonObject.getString("stream_id");
        String mcu_id = jsonObject.getString("mcu_id");
        String mcu_domain = jsonObject.getString("mcu_domain");
        String room_id = jsonObject.getString("room_id");
        Long audio_ssrc = jsonObject.getLong("audio_ssrc");
        Long video_ssrc = jsonObject.getLong("video_ssrc");
        JSONObject options_msg = jsonObject.getJSONObject("options");
        if(client_id==null||stream_id==null||mcu_id==null||mcu_domain==null||room_id==null||options_msg==null){
            log.error("{} msg lack params, msg: {}", CDPublishReadyTask.taskType, msg);
            return;
        }
        Boolean has_video = options_msg.getBoolean("video");
        Boolean has_audio = options_msg.getBoolean("audio");
        Boolean screencast = options_msg.getBoolean("screencast");

        String avRooms_key = MQConstant.REDIS_AVROOMS_KEY;
        String avRoom_hashKey = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
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

        AVStreamInfo avStreamInfo = new AVStreamInfo();
        avStreamInfo.setStream_id(stream_id);
        avStreamInfo.setPublisher_id(client_id);
        avStreamInfo.setRoom_id(room_id);
        avStreamInfo.setRoom_domain(avLogicRoom.getRoom_domain());
        avStreamInfo.setAudioMuted(!has_audio);
        avStreamInfo.setVideoMuted(!has_video);
        avStreamInfo.setScreencast(screencast);
        String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
        //在外域增加发布流的存储，用于后续订阅使用
        if(!RedisUtils.set(redisTemplate,avStreamKey,avStreamInfo)){
            log.error("redis set failed, key: {}, value: {}", avStreamKey, avStreamInfo);
            return;
        }

        //更新逻辑会议室信息-publish_streams
        PublishStreamInfo publishStreamInfo = null;
        if(avLogicRoom.getPublish_streams().containsKey(stream_id)==true){
            publishStreamInfo = avLogicRoom.getPublish_streams().get(stream_id);
        }else{
            publishStreamInfo = new PublishStreamInfo();
            publishStreamInfo.setPublish_clientid(client_id);
            publishStreamInfo.setPublish_streamid(stream_id);
            publishStreamInfo.setScreencast(screencast);
            publishStreamInfo.setAudioMuted(!has_audio);
            publishStreamInfo.setVideoMuted(!has_video);
            publishStreamInfo.setAudio_ssrc(audio_ssrc);
            publishStreamInfo.setVideo_ssrc(video_ssrc);
        }

        MPServerInfo mcu_resource = new MPServerInfo();
        mcu_resource.setEmcu(true);
        mcu_resource.setSrc_domain(mcu_domain);
        mcu_resource.setMp_id(mcu_id);
        publishStreamInfo.addPublish_stream_resource(mcu_resource);
        avLogicRoom.getPublish_streams().put(stream_id, publishStreamInfo);

        //向再会的用户发出stream_add_notice通知，此处需要考虑跨域推送通知
        JSONObject notice_msg = new JSONObject();
        notice_msg.put("type", CDPublishReadyTask.taskNotType);
        notice_msg.put("room_id", room_id);
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
            String mem_domain = notice_mem.getMem_domain();
            if(mem_domain.compareTo(domainBean.getSrcDomain())==0){
                //本域直接发送至客户端
                String mem_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+mem_id;
                log.info("mq send notice {}: {}",mem_bindkey,notice_msg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_bindkey, notice_msg);
            }else{
                //非本域则跨域通知
                DomainRoute domainRoute = domainBean.getDstDomainRoute(mem_domain);
                if(domainRoute!=null){
                    JSONObject crossDomainmsg = new JSONObject();
                    crossDomainmsg.put("type", CDStreamAddNoticeTask.taskType);
                    crossDomainmsg.put("room_domain", avLogicRoom.getRoom_domain());
                    crossDomainmsg.put("notice_client_id", mem_id);
                    crossDomainmsg.put("notice_msg", notice_msg);
                    List<DomainRoute> new_domain_list = new LinkedList<>();
                    new_domain_list.add(domainRoute);
                    JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                    crossDomainmsg.put("domain_route", domain_array);
                    log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                    rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
                }else{
                    log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                            MCUPublishReadyTask.taskNotType, mem_domain, notice_msg);
                }
            }
        }

        //将更新后的逻辑会议室信息存储
        if(RedisUtils.hset(redisTemplate, avRooms_key, avRoom_hashKey, avLogicRoom)==false){
            log.error("redis hset avroominfo failed, key: {} hashket: {}, value: {}",
                    avRooms_key, avRoom_hashKey, avLogicRoom);
            return;
        }
    }
}
