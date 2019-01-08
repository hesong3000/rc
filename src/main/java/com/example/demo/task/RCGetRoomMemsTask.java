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

import java.util.*;

@Component(value=RCGetRoomMemsTask.taskType)
@Scope("prototype")
public class RCGetRoomMemsTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCGetRoomMemsTask.class);
    public final static String taskType = "get_roommems_request";
    public final static String taskResType = "get_roommems_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        log.info("execute RCGetRoomMemsTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String request_client_id = requestMsg.getString("client_id");
        String room_id = requestMsg.getString("room_id");
        String room_domain = requestMsg.getString("room_domain");
        if(request_client_id==null||room_id==null||room_domain==null){
            log.error("{} lack client_id or room_id or room_domain, msg: {}", RCGetRoomMemsTask.taskType, msg);
            return;
        }

        /*
            1、根据有无src_domain字段判定请求是否来自内网
            2、此为定向请求，需判定有无到目的domain的路由服务
         */
        boolean isFromOutterDomain = false;
        String src_domain = requestMsg.getString("src_domain");
        if(src_domain==null)
            isFromOutterDomain = false;
        else
            isFromOutterDomain = true;

        JSONObject response_msg = new JSONObject();
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        int retcode = AVErrorType.ERR_NOERROR;
        String client_sendkey = MQConstant.MQ_CLIENT_KEY_PREFIX+request_client_id;
        if(isFromOutterDomain==false){
            if(room_domain.compareTo(domainBean.getSrcDomain())!=0){
                //请求来自本域，会议室不在本域需要检查有无可用路由
                DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
                if(domainRoute==null){
                    retcode = AVErrorType.ERR_ROUTE_NOTEXIST;
                }else{
                    //发起跨域请求
                    JSONObject outter_request_msg = new JSONObject();
                    outter_request_msg.put("type", RCGetRoomMemsTask.taskType);
                    outter_request_msg.put("client_id", request_client_id);
                    outter_request_msg.put("room_id", room_id);
                    outter_request_msg.put("room_domain", room_domain);
                    outter_request_msg.put("src_domain", domainBean.getSrcDomain());
                    List<DomainRoute> new_domain_list = new LinkedList<>();
                    new_domain_list.add(domainRoute);
                    JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                    outter_request_msg.put("domain_route", domain_array);
                    log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, outter_request_msg);
                    rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", outter_request_msg);
                    return;
                }
            }
        }

        //检查会议室是否存在
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            retcode = AVErrorType.ERR_ROOM_NOTEXIST;
            return;
        }

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(request_client_id) == false)
            retcode =  AVErrorType.ERR_ROOM_KICK;
        response_msg.put("type", RCGetRoomMemsTask.taskResType);
        response_msg.put("retcode", retcode);
        response_msg.put("room_id", room_id);
        if(retcode!=AVErrorType.ERR_NOERROR){
            if(src_domain.compareTo(domainBean.getSrcDomain())==0){
                //请求来自域内，直接发送错误响应给终端
                log.info("mq send client {}: {}", client_sendkey,response_msg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_sendkey, response_msg);
            }
            else{
                //请求来自外域，则发送响应到目的域的RC，由RC中转
                List<DomainRoute> new_domain_list = new LinkedList<>();
                DomainRoute domainRoute = domainBean.getDstDomainRoute(src_domain);
                if(domainRoute==null){
                    log.warn("{} send msg failed, while can not find domain route to {}, msg: {}",
                            RCGetRoomMemsTask.taskType, src_domain, response_msg);
                    return;
                }
                new_domain_list.add(domainRoute);
                JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                response_msg.put("domain_route", domain_array);
                log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, response_msg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", response_msg);
            }
            return;
        }

        List<Map<String,Object>> mem_list = new ArrayList<>();
        Iterator<Map.Entry<String, RoomMemInfo>> mem_iterator = roomMemInfoMap.entrySet().iterator();
        while (mem_iterator.hasNext()) {
            Map.Entry<String, RoomMemInfo> entry = mem_iterator.next();
            RoomMemInfo roomMemInfo = entry.getValue();
            Map<String,Object> mem_info = new HashMap<>();
            mem_info.put("mem_id", roomMemInfo.getMem_id());
            mem_info.put("mem_name", roomMemInfo.getMem_name());
            mem_info.put("mem_online", roomMemInfo.isMem_Online());
            mem_list.add(mem_info);
        }
        JSONArray mem_list_array = JSONArray.parseArray(JSONObject.toJSONString(mem_list));
        response_msg.put("mem_list",mem_list_array);

        //若请求来自本域，则直接发送至目的终端，否则发送至远端RC中转
        if(src_domain.compareTo(domainBean.getSrcDomain())==0) {
            log.info("mq send client {}: {}", client_sendkey, response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_sendkey, response_msg);
        }else{
            List<DomainRoute> new_domain_list = new LinkedList<>();
            DomainRoute domainRoute = domainBean.getDstDomainRoute(src_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed, while can not find domain route to {}, msg: {}",
                        RCGetRoomMemsTask.taskType, src_domain, response_msg);
                return;
            }
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            response_msg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", response_msg);
        }
    }
}
