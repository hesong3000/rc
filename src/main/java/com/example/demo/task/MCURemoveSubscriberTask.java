package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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

@Component(value= MCURemoveSubscriberTask.taskType)
@Scope("prototype")
public class MCURemoveSubscriberTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCURemoveSubscriberTask.class);
    public final static String taskType = "remove_subscriber";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        /*
            1、removeSubscriber过程只做处理，不做失败回复，因为客户端在removeSubscriber前已手动关闭媒体流，
               信令接收通道也有可能关闭，此时如果回复可能会造成客户端处理崩溃
            2、向MCU发送removeSubscriber请求
            3、更新逻辑会议室中有关媒体流相关的信息
         */
        log.info("execute MCURemoveSubscriberTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        String room_id = requestMsg.getString("room_id");
        String publish_stream_id = requestMsg.getString("publish_stream_id");
        if(client_id==null||room_id==null||publish_stream_id==null){
            log.error("remove_publisher msg lack params, msg: {}", msg);
            return;
        }

        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            log.error("remove_publisher failed, avroom not exist, key: {}, hashkey: {}",avRoomsKey,avRoomItem);
            return;
        }

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(client_id) == false){
            log.error("remove_publisher failed, member: {} can not find in room: {}",client_id,avRoomItem);
            return;
        }

        //向MCU发送removeSubscriber请求
        if(avLogicRoom.getPublish_streams().containsKey(publish_stream_id)==true){
            PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(publish_stream_id);
            String mcu_id = publishStreamInfo.getStream_process_mcuid();
            String mcu_sendkey = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
            //向MCU发送请求
            log.info("mq send to mcu {}: {}", mcu_sendkey,requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_sendkey, requestMsg);

            //更新publish_stream信息
            publishStreamInfo.getSubscribers().remove(client_id);

            //更新hash键AV_MPs的mcu使用率
            String mcu_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
            MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, MQConstant.REDIS_MPINFO_HASH_KEY,mcu_hashkey);
            if(mpServerInfo!=null){
                mpServerInfo.setUserd_stream_count(mpServerInfo.getUserd_stream_count()-1);
                if(RedisUtils.hset(redisTemplate, MQConstant.REDIS_MPINFO_HASH_KEY, mcu_hashkey, mpServerInfo)==false){
                    log.error("redis hset failed, key: {}, hashkey: {}, value: {}",
                            MQConstant.REDIS_MPINFO_HASH_KEY,
                            mcu_hashkey,
                            mpServerInfo);
                }
            }

            //更新订阅者的订阅流信息
            avLogicRoom.getRoom_mems().get(client_id).getSubscribe_streams().remove(publish_stream_id);
        }

        //更新会议室信息
        if(RedisUtils.hset(redisTemplate,avRoomsKey,avRoomItem,avLogicRoom)==false){
            log.warn("redis hset failed, key: {}, hashkey: {}, value: {}",avRoomsKey,avRoomItem,avLogicRoom);
        }
    }
}
