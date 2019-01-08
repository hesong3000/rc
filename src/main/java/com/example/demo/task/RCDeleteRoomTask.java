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
    @Autowired
    DomainDefineBean domainBean;

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
        //删除会议室只会发生在本域，因此这个接口不用针对跨域作更改
        log.info("execute RCDeleteRoomTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String room_id = requestMsg.getString("room_id");
        String client_id = requestMsg.getString("client_id");
        String room_domain = requestMsg.getString("room_domain");
        if(room_id==null || client_id==null||room_domain==null){
            log.error("{}: msg lack params, msg: {}", RCDeleteRoomTask.taskType, requestMsg);
            return;
        }

        if(room_domain.compareTo(domainBean.getSrcDomain())!=0)
            return;

        int retcode = AVErrorType.ERR_NOERROR;
        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            log.error("{}: failed, avroom not exist, key: {}, hashkey: {}",RCDeleteRoomTask.taskType, avRoomsKey,avRoomItem);
            retcode = AVErrorType.ERR_ROOM_NOTEXIST;
            return;
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

        //向该用户发送(由于会议室只能在本域创建，则delete响应只需发送到本域)
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
            String mem_domain = roomMemInfo.getMem_domain();
            //向在线用户发送会议室删除通知
            if(roomMemInfo.getMem_id().compareTo(client_id)!=0 && roomMemInfo.isMem_Online()==true
                    && (room_memnum>2)){
                if(mem_domain.compareTo(domainBean.getSrcDomain())==0){
                    String mem_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+roomMemInfo.getMem_id();
                    log.info("mq send notice {}: {}",mem_bindkey,notice_msg);
                    rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_bindkey, notice_msg);
                }else{
                    DomainRoute domainRoute = domainBean.getDstDomainRoute(mem_domain);
                    if(domainRoute==null){
                        log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                                MCUMuteStreamTask.taskType, mem_domain, notice_msg);
                        continue;
                    }

                    JSONObject crossDomainmsg = new JSONObject();
                    crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                    crossDomainmsg.put("client_id", roomMemInfo.getMem_id());
                    crossDomainmsg.put("encap_msg", notice_msg);
                    List<DomainRoute> new_domain_list = new LinkedList<>();
                    new_domain_list.add(domainRoute);
                    JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                    crossDomainmsg.put("domain_route", domain_array);
                    log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                    rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
                }
            }
        }

        //删除逻辑会议室
        RedisUtils.hdel(redisTemplate, avRoomsKey, avRoomItem);
    }
}
