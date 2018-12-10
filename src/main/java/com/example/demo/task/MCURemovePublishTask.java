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
import java.util.Iterator;
import java.util.Map;

@Component(value= MCURemovePublishTask.taskType)
@Scope("prototype")
public class MCURemovePublishTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCURemovePublishTask.class);
    public final static String taskType = "remove_publisher";
    public final static String taskNotType = "stream_removed_notice";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        /*
            1、removePublish过程只做处理，不做失败回复，因为客户端在removePublish前已手动关闭媒体流，
               信令接收通道也有可能关闭，此时如果回复可能会造成客户端处理崩溃
            2、向MCU发送removePublish请求
            3、更新逻辑会议室中有关媒体流相关的信息，向在线的用户发送stream_removed_notice通知
            4、删除AVStreamInfo键
         */
        log.info("execute MCURemovePublishTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        String room_id = requestMsg.getString("room_id");
        String stream_id = requestMsg.getString("stream_id");
        if(client_id==null||room_id==null||stream_id==null){
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

        //向MCU发送removePublish请求
        if(avLogicRoom.getPublish_streams().containsKey(stream_id)==true){
            PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(stream_id);
            if(publishStreamInfo.getPublish_clientid().compareTo(client_id)==0){
                //向MCU发送removePublish请求
                String mcu_id = publishStreamInfo.getStream_process_mcuid();
                String mcu_key = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
                log.info("mq send to mcu {}: {}", mcu_key,requestMsg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_key, requestMsg);
                //检查此项发布流的移除会减少的mcu使用路数(发布流路数1和订阅此流的订阅流路数)
                int stream_reduce = 1+publishStreamInfo.getSubscribers().size();

                //更新hash键AV_MPs的mcu使用率
                String mcu_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
                MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, MQConstant.REDIS_MPINFO_HASH_KEY,mcu_hashkey);
                if(mpServerInfo!=null){
                    mpServerInfo.setUserd_stream_count(mpServerInfo.getUserd_stream_count()-stream_reduce);
                    if(RedisUtils.hset(redisTemplate, MQConstant.REDIS_MPINFO_HASH_KEY, mcu_hashkey, mpServerInfo)==false){
                        log.error("redis hset failed, key: {}, hashkey: {}, value: {}",
                                MQConstant.REDIS_MPINFO_HASH_KEY,
                                mcu_hashkey,
                                mpServerInfo);
                    }
                }

                //更新room member的发布流信息
                avLogicRoom.getRoom_mems().get(client_id).getPublish_streams().remove(stream_id);

                //更新订阅者的订阅流信息
                Iterator<Map.Entry<String, String>> subscriber_it = publishStreamInfo.getSubscribers().entrySet().iterator();
                while (subscriber_it.hasNext()){
                    String subscriberId = subscriber_it.next().getKey();
                    avLogicRoom.getRoom_mems().get(subscriberId).getSubscribe_streams().remove(stream_id);
                }

                //向会议室在线成员发布stream_removed_notice通知
                Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, RoomMemInfo> entry = iterator.next();
                    RoomMemInfo roomMemInfo = entry.getValue();
                    if(roomMemInfo.getMem_id().compareTo(client_id)==0)
                        continue;
                    if(roomMemInfo.isMem_Online()==true){
                        String client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+roomMemInfo.getMem_id();
                        JSONObject stream_remove_notice = new JSONObject();
                        stream_remove_notice.put("type", MCURemovePublishTask.taskNotType);
                        stream_remove_notice.put("client_id", client_id);
                        stream_remove_notice.put("room_id", avLogicRoom.getRoom_id());
                        stream_remove_notice.put("stream_id", stream_id);
                        log.info("mq send to client {}: {}", client_bindkey, stream_remove_notice);
                        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindkey, stream_remove_notice);
                    }
                }

                //删除发布流信息
                avLogicRoom.getPublish_streams().remove(stream_id);

            }else{
                log.warn("remove_publisher failed, stream: {} is published by {}",
                        stream_id,
                        publishStreamInfo.getPublish_clientid());
            }
        }

        //更新逻辑会议室信息
        if(RedisUtils.hset(redisTemplate,avRoomsKey,avRoomItem,avLogicRoom)==false){
            log.warn("redis hset failed, key: {}, hashkey: {}, value: {}",avRoomsKey,avRoomItem,avLogicRoom);
        }

        //删除AVStreamInfo键
        RedisUtils.delKey(redisTemplate, MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id);
    }
}
