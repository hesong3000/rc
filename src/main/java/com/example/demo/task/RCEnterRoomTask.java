package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Component(value=RCEnterRoomTask.taskType)
@Scope("prototype")
public class RCEnterRoomTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCEnterRoomTask.class);
    public final static String taskType = "enter_room";
    public final static String taskResType = "room_enter_reponse";
    public final static String taskNotType = "room_memberin_notice";

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    /*
        1、校验会议室是否存在，会议室是否有该成员
        2、更新会议室中的成员online状态
        3、发送room_enter_reponse到请求成员
        4、发送room_memberin_notice通知到会议室其他上线成员
     */
    private int processRequest(JSONObject requestMsg, Result result){
        String request_room_id = requestMsg.getString("room_id");
        String request_client_id = requestMsg.getString("client_id");
        String room_domain = requestMsg.getString("room_domain");
        if(request_room_id == null||request_client_id == null||room_domain==null)
            return AVErrorType.ERR_PARAM_REQUEST;
        result.request_client_id = request_client_id;
        result.request_room_id = request_room_id;

        boolean isFromOutterDomain = false;
        String request_src_domain = "";
        if(requestMsg.containsKey("src_domain")==true) {
            request_src_domain = requestMsg.getString("src_domain");
            isFromOutterDomain = true;
        }else{
            request_src_domain = domainBean.getSrcDomain();
        }
        result.request_src_domain = request_src_domain;
        result.room_domain = room_domain;
        if(domainBean.getSrcDomain().compareTo(room_domain)!=0 && isFromOutterDomain==false){
            //请求来自本域，会议室不在本域，则发起跨域请求
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute == null)
                return AVErrorType.ERR_ROUTE_NOTEXIST;
            JSONObject outter_request_msg = new JSONObject();
            outter_request_msg.put("type", RCEnterRoomTask.taskType);
            outter_request_msg.put("room_id", request_room_id);
            outter_request_msg.put("client_id", request_client_id);
            outter_request_msg.put("room_domain", room_domain);
            outter_request_msg.put("src_domain", domainBean.getSrcDomain());
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            outter_request_msg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, outter_request_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", outter_request_msg);
            return AVErrorType.ERR_NOERROR;
        }

        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+request_room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }
        result.avLogicRoom = avLogicRoom;

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(request_client_id) == false)
            return AVErrorType.ERR_ROOM_KICK;

        //更新会议室成员状态，并重新存入redis
        avLogicRoom.getRoom_mems().get(request_client_id).setMem_Online(true);
        //更新会议室成员的domain信息
        avLogicRoom.getRoom_mems().get(request_client_id).setMem_domain(request_src_domain);
        if(!RedisUtils.hset(redisTemplate,avRoomsKey,avRoomItem, avLogicRoom)){
            log.error("redis hset avroom failed! {}", avLogicRoom.toString());
            return AVErrorType.ERR_REDIS_STORE;
        }

        return AVErrorType.ERR_NOERROR;
    }

    private int sendResponse(int processCode, Result result){
        JSONObject responseMsg = new JSONObject();
        responseMsg.put("type",RCEnterRoomTask.taskResType);
        if(processCode == AVErrorType.ERR_NOERROR){
            //若发送至外域，则此处不返回
            if(result.room_domain.compareTo(domainBean.getSrcDomain())!=0)
                return processCode;

            AVLogicRoom avLogicRoom = result.avLogicRoom;
            responseMsg.put("retcode",processCode);
            responseMsg.put("room_id", avLogicRoom.getRoom_id());
            responseMsg.put("room_name", avLogicRoom.getRoom_name());
            responseMsg.put("creator_id", avLogicRoom.getCreator_id());
            responseMsg.put("room_domain", avLogicRoom.getRoom_domain());
            responseMsg.put("create_time", avLogicRoom.getCreate_time().getTime());
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
            responseMsg.put("room_id",result.request_room_id);
        }

        if(result.request_src_domain.compareTo(domainBean.getSrcDomain())==0){
            //若请求来自本域，则直接发送至终端
            if(result.request_client_id.length()!=0){
                String send_routekey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.request_client_id;
                log.info("mq send to client {}: {}",send_routekey,responseMsg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, send_routekey, responseMsg);
            }
        }else{
            //若请求来自外域，则通过远端RC代为转发，并对消息进行封装
            DomainRoute domainRoute = domainBean.getDstDomainRoute(result.request_src_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        RCEnterRoomTask.taskType, result.request_src_domain, responseMsg);
                return processCode;
            }

            JSONObject crossDomainmsg = new JSONObject();
            crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
            crossDomainmsg.put("client_id", result.request_client_id);
            crossDomainmsg.put("encap_msg", responseMsg);
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            crossDomainmsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
        }
        return processCode;
    }

    private int sendNotice(int processCode, Result result){
        if(processCode != AVErrorType.ERR_NOERROR)
            return -1;

        //若发送至外域，则此处不处理
        if(result.room_domain.compareTo(domainBean.getSrcDomain())!=0)
            return -1;

        AVLogicRoom avLogicRoom = result.avLogicRoom;
        JSONObject notice_msg = new JSONObject();
        notice_msg.put("type", RCEnterRoomTask.taskNotType);
        notice_msg.put("room_id", avLogicRoom.getRoom_id());
        notice_msg.put("client_id", result.request_client_id);
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo roomMemInfo = entry.getValue();
            String mem_id = roomMemInfo.getMem_id();
            boolean mem_online = roomMemInfo.isMem_Online();
            if(mem_id.compareTo(result.request_client_id)==0 || mem_online == false)
                continue;
            String mem_routingkey = MQConstant.MQ_CLIENT_KEY_PREFIX+mem_id;
            if(roomMemInfo.getMem_domain().compareTo(domainBean.getSrcDomain())==0){
                log.info("mq send notice {}: {}",mem_routingkey,notice_msg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_routingkey, notice_msg);
            }else{
                DomainRoute domainRoute = domainBean.getDstDomainRoute(roomMemInfo.getMem_domain());
                if(domainRoute!=null){
                    JSONObject crossDomainmsg = new JSONObject();
                    crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                    crossDomainmsg.put("client_id", mem_id);
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

        return processCode;
    }

    @Override
    @Transactional
    public void run() {
        log.info("execute RCEnterRoomTask at {}", new Date());
        try{
            JSONObject requestMsg = JSON.parseObject(msg);
            int processCode = AVErrorType.ERR_NOERROR;
            Result result = new Result();
            processCode = processRequest(requestMsg,result);
            processCode = sendResponse(processCode,result);
            sendNotice(processCode,result);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    class Result{
        String request_room_id = "";
        String request_client_id = "";
        String request_src_domain = "";
        String room_domain = "";
        AVLogicRoom avLogicRoom;
    }
}
