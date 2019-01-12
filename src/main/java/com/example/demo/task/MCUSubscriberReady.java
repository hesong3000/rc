package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
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

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@Component(value= MCUSubscriberReady.taskType)
@Scope("prototype")
public class MCUSubscriberReady extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCUSubscriberReady.class);
    public final static String taskType = "subscribe_ready";
    public final static String taskFailType = "remove_subscriber";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;
    @Override
    public void run() {
        /*
            1、检查客户端是否存活，若存活则发送subscriber_ready到客户端，否则发送remove_subscriber到mcu
            2、更新AV_MPs哈希键和AV_Rooms哈希键，必须正确，错误打印ERROR级别日志，为逻辑错误，需要检查原因
         */
        log.info("execute MCUSubscriberReady at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        String publish_stream_id = jsonObject.getString("publish_stream_id");
        String mcu_id = jsonObject.getString("mcu_id");
        String roomID = "";
        String roomDomain = "";
        boolean isFromOutter = false;
        if(jsonObject.containsKey("src_domain")==true){
            roomID = jsonObject.getString("room_id");
            roomDomain = jsonObject.getString("room_domain");
            isFromOutter = true;
        }

        if(isFromOutter==false){
            //检查AV_Stream:[StreamID]键，获取发布流所在会议室以及媒体流属性信息，用于更新会议室
            String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+publish_stream_id;
            AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avStreamKey);
            if(avStreamInfo==null){
                //此处获取不到则属于逻辑错误，检查BUG
                log.error("can not get streaminfo, msg: {}", msg);
                return;
            }

            roomID = avStreamInfo.getRoom_id();
            roomDomain = avStreamInfo.getRoom_domain();
            //更新MCU使用率信息
            String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
            String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
            MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
            if(mpServerInfo!=null){
                mpServerInfo.addMcuUseResource(roomID, publish_stream_id,1);
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

        String avUserKey = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        //检查客户端是否存活
        AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate,avUserKey);
        if(avUserInfo==null){
            //可能是级联订阅的ready消息，后续不处理此消息
            return;
        }

        //透传subscriber_ready消息到客户端
        String client_bindkey = avUserInfo.getBinding_key();
        log.warn("mq send msg to client  {}: {}", client_bindkey,jsonObject);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindkey,jsonObject);

        if(roomDomain.compareTo(domainBean.getSrcDomain())!=0){
            //请求发送到room所在域
            DomainRoute domainRoute = domainBean.getDstDomainRoute(roomDomain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        MCUSubscriberReady.taskType, roomDomain, jsonObject);
                return;
            }
            jsonObject.put("room_id", roomID);
            jsonObject.put("room_domain", roomDomain);
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            jsonObject.put("src_domain", domainBean.getSrcDomain());
            jsonObject.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, jsonObject);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", jsonObject);
            return;
        }

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
            roomMemInfo.getSubscribe_streams().put(publish_stream_id,publish_stream_id);
        }else{
            //此处获取不到则属于逻辑错误，检查BUG
            log.error("can not get roomMemInfo, msg: {}", msg);
            return;
        }

        //更新逻辑会议室信息-publish_streams
        PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(publish_stream_id);
        publishStreamInfo.getSubscribers().put(client_id,client_id);
        //将更新后的逻辑会议室信息存储
        if(RedisUtils.hset(redisTemplate, avRooms_key, avRoom_hashKey, avLogicRoom)==false){
            log.error("redis hset avroominfo failed, key: {} hashket: {}, value: {}",
                    avRooms_key, avRoom_hashKey, avLogicRoom);
            return;
        }
    }
}
