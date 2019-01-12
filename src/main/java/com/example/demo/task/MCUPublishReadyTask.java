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
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        /*
            1、检测客户端是否存活，若存活则发送publish_ready到客户端，若不存活则发送removePublish请求到MCU，逻辑退出
            2、更新AV_MPs哈希键和AV_Rooms哈希键，必须正确，错误打印ERROR级别日志，为逻辑错误，需要检查原因
            3、向会议室中再会的各成员发送stream_add_notice通知
         */
        /*
            1、跨域级联增加
            publish_ready只是本域行为，但根据room_domain需要产生crossdomain_publish_ready消息进行跨域传递
         */

        log.info("execute MCUPublishReadyTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        String stream_id = jsonObject.getString("stream_id");
        String mcu_id = jsonObject.getString("mcu_id");
        Long audio_ssrc = jsonObject.getLong("audio_ssrc");
        Long video_ssrc = jsonObject.getLong("video_ssrc");
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

        //查询发布流信息，由此确定room_id和room_domain，增加发布流信息是mcu不涉及业务，只能在redis中存储
        String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avStreamKey);
        if(avStreamInfo==null){
            //此处获取不到则属于逻辑错误，检查BUG
            log.error("can not get streaminfo, msg: {}", msg);
            return;
        }
        String room_domain = avStreamInfo.getRoom_domain();
        String room_id = avStreamInfo.getRoom_id();

        //查询处理发布流的mcu信息，无法查询到是BUG
        String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
        MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
        if(mpServerInfo == null){
            log.error("can not get MPServerInfo, msg: {}", msg);
            return;
        }

        //更新MCU使用率信息
        //若publishstream在本域处理，则更新mcu使用率信息
        //流发布成功，占用一路mcu资源
        mpServerInfo.addMcuUseResource(room_id, stream_id,1);

        //将MCU的更新信息存储至Redis
        if(RedisUtils.hset(redisTemplate, av_mps_key, av_mp_hashkey, mpServerInfo)==false){
            log.error("redis hset mpserver info failed, key: {} hashket: {}, value: {}",
                    av_mps_key, av_mp_hashkey, mpServerInfo);
        }

        if(room_domain.compareTo(domainBean.getSrcDomain())!=0){
            //若room_domain不是本域，则跨域发送请求
            JSONObject crossdomain_msg = (JSONObject)jsonObject.clone();
            crossdomain_msg.remove("type");
            crossdomain_msg.put("type", CDPublishReadyTask.taskType);
            crossdomain_msg.put("room_id", room_id);
            crossdomain_msg.put("mcu_domain", mpServerInfo.getSrc_domain());
            crossdomain_msg.put("audio_ssrc", audio_ssrc);
            crossdomain_msg.put("video_ssrc", video_ssrc);
            JSONObject option_msg = new JSONObject();
            option_msg.put("video", !avStreamInfo.getVideoMuted());
            option_msg.put("audio", !avStreamInfo.getAudioMuted());
            option_msg.put("screencast", avStreamInfo.getScreencast());
            crossdomain_msg.put("options", option_msg);
            crossdomain_msg.put("src_domain", domainBean.getSrcDomain());
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        RCEnterRoomTask.taskType, room_domain, crossdomain_msg);
                return;
            }
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            crossdomain_msg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossdomain_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossdomain_msg);
        }else{
            //若room_domain是本域，则更新会议室信息
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
            PublishStreamInfo publishStreamInfo = null;
            if(avLogicRoom.getPublish_streams().containsKey(stream_id)==true){
                publishStreamInfo = avLogicRoom.getPublish_streams().get(stream_id);
            }else{
                publishStreamInfo = new PublishStreamInfo();
                publishStreamInfo.setPublish_clientid(client_id);
                publishStreamInfo.setPublish_streamid(stream_id);
                publishStreamInfo.setScreencast(avStreamInfo.getScreencast());
                publishStreamInfo.setAudioMuted(avStreamInfo.getAudioMuted());
                publishStreamInfo.setVideoMuted(avStreamInfo.getVideoMuted());
                publishStreamInfo.setAudio_ssrc(audio_ssrc);
                publishStreamInfo.setVideo_ssrc(video_ssrc);
            }

            try {
                MPServerInfo mcu_resource = (MPServerInfo)mpServerInfo.clone();
                publishStreamInfo.addPublish_stream_resource(mcu_resource);
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            avLogicRoom.getPublish_streams().put(stream_id, publishStreamInfo);

            //向再会的用户发出stream_add_notice通知，此处需要考虑跨域推送通知
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
}
