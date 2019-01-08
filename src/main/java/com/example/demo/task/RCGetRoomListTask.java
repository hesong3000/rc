package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVRoomInfo;
import com.example.demo.po.DomainRoute;
import com.example.demo.po.MPServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Component(value=RCGetRoomListTask.taskType)
@Scope("prototype")
public class RCGetRoomListTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RCGetRoomListTask.class);
    public final static String taskType = "get_roomlist_request";
    public final static String taskResType = "get_roomlist_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    @Transactional
    public void run() {
        log.info("execute RCGetRoomListTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String client_id = jsonObject.getString("client_id");
        String request_src_domain = "";
        //判断是否为跨域请求
        boolean isFromOutterDomain = false;
        if(jsonObject.containsKey("src_domain")==true) {
            request_src_domain = jsonObject.getString("src_domain");
            isFromOutterDomain = true;
        }else
            request_src_domain = domainBean.getSrcDomain();

        //若来自本域则向外域广播请求
        if(isFromOutterDomain==false){
            JSONObject outter_msg = new JSONObject();
            outter_msg.put("type", RCGetRoomListTask.taskType);
            outter_msg.put("client_id", client_id);
            outter_msg.put("src_domain", domainBean.getSrcDomain());
            List<DomainRoute> new_domain_list = new LinkedList<>();
            DomainRoute domainRoute = domainBean.getBroadcastDomainRoute();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            outter_msg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, outter_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", outter_msg);
        }

        //根据用户ID获得所在会议室集合键：(key: AV_User_Room_[UserID])
        String userRoomKey = MQConstant.REDIS_USER_ROOM_KEY_PREFIX+client_id;
        Map<Object, Object> userRoomsMap = RedisUtils.hmget(redisTemplate, userRoomKey);
        Iterator<Map.Entry<Object, Object>> userRoom_iterator = userRoomsMap.entrySet().iterator();
        List<Map<String,Object>> roominfo_list = new ArrayList<>();
        while(userRoom_iterator.hasNext()){
            AVRoomInfo avRoomInfo = (AVRoomInfo)userRoom_iterator.next().getValue();
            Map<String,Object> room_info = new HashMap<>();
            room_info.put("room_id", avRoomInfo.getRoom_id());
            room_info.put("room_name", avRoomInfo.getRoom_name());
            room_info.put("creator_id", avRoomInfo.getCreator_id());
            room_info.put("create_time", avRoomInfo.getCreate_time());
            room_info.put("mem_num", avRoomInfo.getMem_num());
            room_info.put("room_domain",domainBean.getSrcDomain());
            //AV_User_Room：[UserID]的会议室信息只保留在逻辑会议室所在的域
            roominfo_list.add(room_info);
        }

        JSONArray room_list_array = JSONArray.parseArray(JSONObject.toJSONString(roominfo_list));
        JSONObject responseMsg = new JSONObject();
        responseMsg.put("type",RCGetRoomListTask.taskResType);
        responseMsg.put("client_id", client_id);
        responseMsg.put("room_list",room_list_array);

        //若消息来自本域直接将消息透传给用户
        if(request_src_domain.compareTo(domainBean.getSrcDomain())==0){
            String send_routekey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
            log.info("mq send to client {}: {}",send_routekey,responseMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, send_routekey, responseMsg);
        }else{
            //若请求来自外域，若有属于该用户的消息，则将回复消息投递至远端RC
            if(roominfo_list.size()>0){
                DomainRoute domainRoute = domainBean.getDstDomainRoute(request_src_domain);
                JSONObject crossDomainmsg = new JSONObject();
                crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                crossDomainmsg.put("client_id", client_id);
                crossDomainmsg.put("encap_msg", responseMsg);
                List<DomainRoute> new_domain_list = new LinkedList<>();
                new_domain_list.add(domainRoute);
                JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                crossDomainmsg.put("domain_route", domain_array);
                log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
            }
        }
    }
}
