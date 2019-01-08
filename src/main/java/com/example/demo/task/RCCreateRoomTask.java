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
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Component(value=RCCreateRoomTask.taskType)
@Scope("prototype")
public class RCCreateRoomTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RCCreateRoomTask.class);
    public final static String taskType = "create_room";
    public final static String taskResType = "room_create_reponse";
    public final static String taskNotType = "room_invite_notice";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    /*
        1、处理create_room的请求消息，加入hash表: AV_Rooms的item: AV_Room_[RoomID]
        2、1步骤完成后，将会议室相关信息加入普通键值对： AV_User_Room_[UserID]，用于查看用户所属的会议室列表，可随时进入会议室
           但一个用户只能同时进入一个会议室
        3、向创建者发送room_create_reponse，向其他会议成员发送room_invite_notice通知。
     */
    private int processRequest(JSONObject requestMsg, Result result){
        AVLogicRoom avLogicRoom = new AVLogicRoom();
        String room_id = requestMsg.getString("room_id");
        avLogicRoom.setRoom_id(room_id);
        String room_name = requestMsg.getString("room_name");
        avLogicRoom.setRoom_name(room_name);
        String creator_id = requestMsg.getString("creator_id");
        avLogicRoom.setCreator_id(creator_id);
        if(room_id != null)
            result.create_room_id = room_id;
        if(creator_id != null)
            result.creator_user_id = creator_id;

        if(room_id==null || room_id.length()==0)
            return AVErrorType.ERR_PARAM_REQUEST;
        if(RedisUtils.hHasKey(redisTemplate, MQConstant.REDIS_AVROOMS_KEY,MQConstant.REDIS_ROOM_KEY_PREFIX+room_id)){
            return AVErrorType.ERR_ROOMID_CONFLICT;
        }
        Date create_time = new Date();
        avLogicRoom.setCreate_time(create_time);
        avLogicRoom.setRoom_domain(domainBean.getSrcDomain());
        Map<String, RoomMemInfo> room_mems = new HashMap<>();
        JSONArray user_list = requestMsg.getJSONArray("mem_list");
        List<String> userRoomToInsert = new LinkedList<>();
        for(int index=0;index<user_list.size();index++){
            RoomMemInfo roomMemInfo = new RoomMemInfo();
            String client_id = user_list.getJSONObject(index).getString("mem_id");
            roomMemInfo.setMem_id(client_id);
            String user_name = user_list.getJSONObject(index).getString("mem_name");
            roomMemInfo.setMem_name(user_name);
            if(creator_id.compareTo(client_id)==0){
                roomMemInfo.setMem_Online(true);
                roomMemInfo.setMem_domain(domainBean.getSrcDomain());
            }else{
                roomMemInfo.setMem_Online(false);
            }
            userRoomToInsert.add(client_id);
            room_mems.put(client_id,roomMemInfo);
        }
        avLogicRoom.setRoom_mems(room_mems);
        result.avLogicRoom = avLogicRoom;

        //加入逻辑会议室hash键: (key: AV_Rooms, item: AV_Room_[RoomID])
        if(!RedisUtils.hset(redisTemplate,MQConstant.REDIS_AVROOMS_KEY,MQConstant.REDIS_ROOM_KEY_PREFIX+room_id, avLogicRoom)){
            log.error("redis hset avroom failed! {}", avLogicRoom.toString());
            return AVErrorType.ERR_REDIS_STORE;
        }

        //扩散写方式加入各用户的所在会议室hash键：(key: AV_User_Room_[UserID] hashkey: roomId
        Iterator iter = userRoomToInsert.iterator();
        while(iter.hasNext()){
            AVRoomInfo avRoomInfo = new AVRoomInfo();
            avRoomInfo.setRoom_id(room_id);
            avRoomInfo.setRoom_name(room_name);
            avRoomInfo.setCreator_id(creator_id);
            avRoomInfo.setCreate_time(create_time.getTime());
            avRoomInfo.setMem_num(room_mems.size());
            String client_id = (String)iter.next();
            String userRoomKey = MQConstant.REDIS_USER_ROOM_KEY_PREFIX+client_id;
            String userroom_hashkey = room_id;
            if(RedisUtils.hset(redisTemplate,userRoomKey,userroom_hashkey,avRoomInfo)==false){
                log.error("redis hset failed, key: {}, hashkey: {}, value: {}",
                        userRoomKey,
                        userroom_hashkey,
                        avRoomInfo);
                continue;
            }
        }
        return AVErrorType.ERR_NOERROR;
    }

    //发送room_create_reponse逻辑，只能在本域创建会议，但可邀请外域成员，因此创建会议响应只需回复到本域，不涉及跨域操作
    private int sendResponse(int processCode, Result result){
        JSONObject responseMsg = new JSONObject();
        responseMsg.put("type",RCCreateRoomTask.taskResType);
        if(processCode == AVErrorType.ERR_NOERROR){
            AVLogicRoom avLogicRoom = result.avLogicRoom;
            responseMsg.put("retcode",processCode);
            responseMsg.put("room_id", avLogicRoom.getRoom_id());
            responseMsg.put("room_name", avLogicRoom.getRoom_name());
            responseMsg.put("creator_id", avLogicRoom.getCreator_id());
            responseMsg.put("create_time", avLogicRoom.getCreate_time().getTime());
            responseMsg.put("room_domain", avLogicRoom.getRoom_domain());
            List<Map<String,Object>> mem_list = new ArrayList<>();
            Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, RoomMemInfo> entry = iterator.next();
                RoomMemInfo roomMemInfo = entry.getValue();
                Map<String,Object> mem_info = new HashMap<>();
                mem_info.put("mem_id", roomMemInfo.getMem_id());
                mem_info.put("mem_name", roomMemInfo.getMem_name());
                mem_info.put("mem_online", roomMemInfo.isMem_Online());
                mem_list.add(mem_info);
            }
            JSONArray mem_list_array = JSONArray.parseArray(JSONObject.toJSONString(mem_list));
            responseMsg.put("mem_list",mem_list_array);
        }else{
            responseMsg.put("retcode",processCode);
            responseMsg.put("room_id",result.create_room_id);
        }

        if(result.creator_user_id.length()!=0){
            String send_routekey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.creator_user_id;
            log.info("mq send response {}: {}",send_routekey,responseMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, send_routekey, responseMsg);
        }
        return processCode;
    }

    //向非creator_id的会议室成员发送room_invite_notice
    private int sendNotice(int processCode, Result result){
        if(processCode != AVErrorType.ERR_NOERROR)
            return -1;

        AVLogicRoom avLogicRoom = result.avLogicRoom;
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo roomMemInfo = entry.getValue();
            String mem_id = roomMemInfo.getMem_id();
            if(avLogicRoom.getCreator_id().compareTo(mem_id)==0)
                continue;
            //检查用户是否在线（只给在线用户发送会议邀请） 邀请者此处为会议创建者
            String userKey = MQConstant.REDIS_USER_KEY_PREFIX+mem_id;
            AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate,userKey);
            if(avUserInfo!=null){
                JSONObject response_msg = new JSONObject();
                response_msg.put("type", RCCreateRoomTask.taskNotType);
                response_msg.put("creator_id", avLogicRoom.getCreator_id());
                response_msg.put("invitor_id", avLogicRoom.getCreator_id());
                response_msg.put("room_id", avLogicRoom.getRoom_id());
                response_msg.put("room_name", avLogicRoom.getRoom_name());
                response_msg.put("room_domain", avLogicRoom.getRoom_domain());
                response_msg.put("mem_num", avLogicRoom.getRoom_mems().size());
                //若用户属于本域，则直接发送
                if(avUserInfo.getSrc_domain().compareTo(domainBean.getSrcDomain())==0){
                    log.info("mq send to client {}: {}", avUserInfo.getBinding_key(),response_msg);
                    rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, avUserInfo.getBinding_key(), response_msg);
                }else{
                    //若用户不属于本域，则通过远端RC转发
                    String client_dst_domain = avUserInfo.getSrc_domain();
                    DomainRoute domainRoute = domainBean.getDstDomainRoute(client_dst_domain);
                    if(domainRoute==null){
                        log.warn("{} send msg failed while can not find avilable domainroute to {}, msg: {}",
                                RCCreateRoomTask.taskNotType, client_dst_domain, response_msg);
                        continue;
                    }

                    JSONObject crossDomainmsg = new JSONObject();
                    crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                    crossDomainmsg.put("client_id", mem_id);
                    crossDomainmsg.put("encap_msg", response_msg);
                    List<DomainRoute> new_domain_list = new LinkedList<>();
                    new_domain_list.add(domainRoute);
                    JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                    crossDomainmsg.put("domain_route", domain_array);
                    log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                    rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
                }
            }else{
                log.info("send room_invite_notice ignore, while user: {} is offline", mem_id);
            }
        }
        return processCode;
    }

    @Override
    @Transactional
    public void run() {
        log.info("execute RCCreateRoomTask at {}", new Date());
        try {
            JSONObject requestMsg = JSON.parseObject(msg);
            int processCode = AVErrorType.ERR_NOERROR;
            Result result = new Result();
            processCode = processRequest(requestMsg,result);
            processCode = sendResponse(processCode,result);
            sendNotice(processCode,result);
        }
        catch (Exception e){
            e.printStackTrace();
            return;
        }
    }

    class Result{
        String create_room_id = "";
        String creator_user_id = ""; //get bindkey from creator_user_id
        AVLogicRoom avLogicRoom;
    }
}
