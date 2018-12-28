package com.example.demo.task;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
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

@Component(value=RCDeleteRoomTask.taskType)
@Scope("prototype")
public class RCDeleteRoomTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RCDeleteRoomTask.class);
    public final static String taskType = "delete_room";
    public final static String taskResType = "delete_room_response";
    public final static String taskRemovePublishTask = "remove_publisher";
    public final static String taskNotType = "room_delete_notice";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
        /*
            1、校验信令参数
            2、判断client_id是否为creator_id
            3、查看逻辑会议室所有发布流，向MCU发送removePublish请求（不需要发送removeSubscriber， mcu会自行处理），
               此时不发送stream remove notice通知
            4、向所有在会会议室成员发送room_delete_notice通知
            5、删除逻辑会议室
            6、更新mcu的使用率信息，以及会议室信息
         */
        log.info("execute RCDeleteRoomTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String room_id = requestMsg.getString("room_id");
        String client_id = requestMsg.getString("client_id");
        if(room_id==null || client_id==null){
            log.error("{}: msg lack params, msg: {}", RCDeleteRoomTask.taskType, requestMsg);
            return;
        }

        int retcode = AVErrorType.ERR_NOERROR;
        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            log.error("{}: failed, avroom not exist, key: {}, hashkey: {}",RCDeleteRoomTask.taskType, avRoomsKey,avRoomItem);
            retcode = AVErrorType.ERR_ROOM_NOTEXIST;
        }

        //检查会议室创建者是否为本用户
        String creator_id = avLogicRoom.getCreator_id();
        if(creator_id.compareTo(client_id)!=0){
            log.error("{}: failed, client is not the creator, msg:{}",RCDeleteRoomTask.taskType,msg);
            retcode = AVErrorType.ERR_ROOM_KICK;
        }

        //检查当前会议室是否有用户正在会议中,此种状态不能删除
        Iterator<Map.Entry<String, RoomMemInfo>> check_roommem_it = avLogicRoom.getRoom_mems().entrySet().iterator();
        while(check_roommem_it.hasNext()){
            RoomMemInfo roomMemInfo = check_roommem_it.next().getValue();
            if(roomMemInfo.isMem_Online()==true && client_id.compareTo(roomMemInfo.getMem_id())!=0){
                retcode = AVErrorType.ERR_ROOM_BUSY;
                break;
            }
        }

        //向该用户发送
        String request_client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
        JSONObject response_msg = new JSONObject();
        response_msg.put("type", RCDeleteRoomTask.taskResType);
        response_msg.put("retcode", retcode);
        response_msg.put("client_id", client_id);
        response_msg.put("room_id", room_id);
        if(avLogicRoom.getRoom_mems().size()>2)
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, request_client_bindkey, response_msg);

        if(retcode!=AVErrorType.ERR_NOERROR)
            return;

        //查看发布流
        Map<String, Integer> process_mcu_map = new HashMap<>();
        Map<String, PublishStreamInfo>publishStreamInfoMap = avLogicRoom.getPublish_streams();
        Iterator<Map.Entry<String, PublishStreamInfo>> publishstream_it = publishStreamInfoMap.entrySet().iterator();
        while (publishstream_it.hasNext()){
            PublishStreamInfo publishStreamInfo = publishstream_it.next().getValue();
            int use_resource_count = publishStreamInfo.getSubscribers().size()+1;

            if(process_mcu_map.containsKey(publishStreamInfo.getStream_process_mcuid())){
                Integer all_use_resource_count = process_mcu_map.get(publishStreamInfo.getStream_process_mcuid());
                process_mcu_map.put(publishStreamInfo.getStream_process_mcuid(), all_use_resource_count+use_resource_count);
            }else{
                process_mcu_map.put(publishStreamInfo.getStream_process_mcuid(), use_resource_count);
            }

            //向mcu发送取消发布流请求
            String mcu_senkey = MQConstant.MQ_MCU_KEY_PREFIX+publishStreamInfo.getStream_process_mcuid();
            JSONObject removePublish_msg = new JSONObject();
            removePublish_msg.put("type",RCDeleteRoomTask.taskRemovePublishTask);
            removePublish_msg.put("client_id", publishStreamInfo.getPublish_clientid());
            removePublish_msg.put("stream_id", publishStreamInfo.getPublish_streamid());
            log.info("mq send to mcu {}: {}", mcu_senkey,removePublish_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_senkey, removePublish_msg);

            //删除AV_Stream:[StreamID]键
            RedisUtils.delKey(redisTemplate, MQConstant.REDIS_STREAM_KEY_PREFIX+publishStreamInfo.getPublish_streamid());
        }

        //更新MCU使用率以及会议室信息
        Iterator<Map.Entry<String, Integer>> procmcu_it = process_mcu_map.entrySet().iterator();
        String mpservers_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        while(procmcu_it.hasNext()){
            Map.Entry<String, Integer> entry = procmcu_it.next();
            String proc_mcu_id = entry.getKey();
            Integer proc_mcu_res_count = entry.getValue();
            String mpserver_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+proc_mcu_id;
            MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, mpservers_key,mpserver_hashkey);
            if(mpServerInfo!=null){
                mpServerInfo.setUserd_stream_count(mpServerInfo.getUserd_stream_count()-proc_mcu_res_count);
                mpServerInfo.getRoom_list().remove(room_id);
                RedisUtils.hset(redisTemplate,mpservers_key,mpserver_hashkey,mpServerInfo);
            }
        }

        //向其他再会用户发送room_delete_notice通知
        JSONObject notice_msg = new JSONObject();
        notice_msg.put("type", RCDeleteRoomTask.taskNotType);
        notice_msg.put("room_id", avLogicRoom.getRoom_id());

        int room_memnum = avLogicRoom.getRoom_mems().size();
        Iterator<Map.Entry<String, RoomMemInfo>> roommem_it = avLogicRoom.getRoom_mems().entrySet().iterator();
        while(roommem_it.hasNext()){
            RoomMemInfo roomMemInfo = roommem_it.next().getValue();
            //删除用户所在会议室
            String avuserroom_key = MQConstant.REDIS_USER_ROOM_KEY_PREFIX+roomMemInfo.getMem_id();
            String avroom_hashkey = avLogicRoom.getRoom_id();
            RedisUtils.hdel(redisTemplate,avuserroom_key,avroom_hashkey);

            //向在线用户发送会议室删除通知
            if(roomMemInfo.getMem_id().compareTo(client_id)!=0 && roomMemInfo.isMem_Online()==true
                    && (room_memnum>2)){
                String client_sendkey = MQConstant.MQ_CLIENT_KEY_PREFIX+roomMemInfo.getMem_id();
                log.info("mq send to client {}: {}", client_sendkey, notice_msg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_sendkey, notice_msg);
            }
        }

        //删除逻辑会议室
        RedisUtils.hdel(redisTemplate, avRoomsKey, avRoomItem);
    }
}
