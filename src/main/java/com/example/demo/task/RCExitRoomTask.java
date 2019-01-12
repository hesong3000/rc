package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVLogicRoom;
import com.example.demo.po.DomainRoute;
import com.example.demo.po.RoomMemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.*;

@Component(value=RCExitRoomTask.taskType)
@Scope("prototype")
public class RCExitRoomTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RCExitRoomTask.class);
    public final static String taskType = "exit_room";
    public final static String taskNotType = "room_memberout_notice";
    public final static String deleteTaskType = "delete_room";
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        log.info("execute RCExitRoomTask at {}", new Date());
        try{
            JSONObject requestMsg = JSON.parseObject(msg);
            int processCode = AVErrorType.ERR_NOERROR;
            Result result = new Result();
            processCode = processRequest(requestMsg,result);
            processCode = sendNotice(processCode,result);
            processFollow(processCode,result);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private int processRequest(JSONObject requestMsg, Result result){
        String request_room_id = requestMsg.getString("room_id");
        String request_client_id = requestMsg.getString("client_id");
        String room_domain = requestMsg.getString("room_domain");
        String request_client_domain = "";
        if(request_room_id == null || request_client_id == null||room_domain==null){
            log.error("{}: request msg invalid, msg: {}", RCExitRoomTask.taskType, requestMsg);
            return AVErrorType.ERR_PARAM_REQUEST;
        }

        if(requestMsg.containsKey("src_domain")==true)
            request_client_domain = requestMsg.getString("src_domain");
        else
            request_client_domain = domainBean.getSrcDomain();

        result.request_client_domain = request_client_domain;
        result.request_client_id = request_client_id;
        result.request_room_id = request_room_id;
        result.room_domain = room_domain;

        if(room_domain.compareTo(domainBean.getSrcDomain())!=0){
            DomainRoute domainRoute = domainBean.getDstDomainRoute(result.room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        RCExitRoomTask.taskType, room_domain, requestMsg);
                return AVErrorType.ERR_ROUTE_NOTEXIST;
            }
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            requestMsg.put("src_domain", domainBean.getSrcDomain());
            requestMsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", requestMsg);
            return AVErrorType.ERR_NOERROR;
        }

        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+request_room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate,avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            log.error("{}: failed, room not exist msg: {}", RCExitRoomTask.taskType, requestMsg);
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }
        result.avLogicRoom = avLogicRoom;

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(request_client_id) == false){
            log.error("{}: failed, member has kickout msg: {}", RCExitRoomTask.taskType, requestMsg);
            return AVErrorType.ERR_ROOM_KICK;
        }

        result.room_memnum = avLogicRoom.getRoom_mems().size();
        result.creator_id = avLogicRoom.getCreator_id();

        //更新会议室成员状态，并重新存入redis
        avLogicRoom.getRoom_mems().get(request_client_id).setMem_Online(false);

        log.info("hset key: {}, hashkey: {}, value: {}", avRoomsKey, avRoomItem, JSON.toJSONString(avLogicRoom));
        if(!RedisUtils.hset(redisTemplate,avRoomsKey,avRoomItem, avLogicRoom)){
            log.error("{}: redis hset avroom failed! {}",  RCExitRoomTask.taskType, avLogicRoom.toString());
            return AVErrorType.ERR_REDIS_STORE;
        }
        return AVErrorType.ERR_NOERROR;
    }

    private int sendNotice(int processCode, Result result){
        if(processCode != AVErrorType.ERR_NOERROR)
            return -1;
        if(result.room_domain.compareTo(domainBean.getSrcDomain())!=0)
            return -1;
        //广播退出通知
        boolean isAllMemExit = true;
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = result.avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo roomMemInfo = entry.getValue();
            String mem_id = roomMemInfo.getMem_id();
            boolean mem_online = roomMemInfo.isMem_Online();
            String mem_domain = roomMemInfo.getMem_domain();
            if(mem_id.compareTo(result.request_client_id) ==0 || mem_online == false)
                continue;
            if(mem_online == true)
                isAllMemExit = false;
            JSONObject notice_msg = new JSONObject();
            notice_msg.put("type", RCExitRoomTask.taskNotType);
            notice_msg.put("room_id", result.avLogicRoom.getRoom_id());
            notice_msg.put("client_id", result.request_client_id);
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
                    crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                    crossDomainmsg.put("client_id", roomMemInfo.getMem_id());
                    crossDomainmsg.put("encap_msg", notice_msg);
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
        result.isAllMemExit = isAllMemExit;
        return processCode;
    }

    private int processFollow(int processCode, Result result){

        if(processCode!=AVErrorType.ERR_NOERROR)
            return processCode;
        if(result.room_domain.compareTo(domainBean.getSrcDomain())!=0)
            return processCode;

        if(result.isAllMemExit==true && result.room_memnum==2){
            JSONObject delete_room_msg = new JSONObject();
            delete_room_msg.put("type", RCExitRoomTask.deleteTaskType);
            delete_room_msg.put("client_id", result.creator_id);
            delete_room_msg.put("room_id", result.request_room_id);
            log.info("mq send RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,delete_room_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, delete_room_msg);
        }
        return processCode;
    }

    class Result{
        String request_room_id = "";
        String request_client_id = "";
        String request_client_domain = "";
        String room_domain = "";
        boolean isAllMemExit = false;
        int room_memnum = 0;
        String creator_id = "";
        AVLogicRoom avLogicRoom;
    }
}
