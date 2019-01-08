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

@Component(value= MCUMuteStreamTask.taskType)
@Scope("prototype")
public class MCUMuteStreamTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCUMuteStreamTask.class);
    public final static String taskType = "mute_stream_request";
    public final static String taskResType = "mute_stream_response";
    public final static String taskNotType = "stream_muted_notice"; //只有publishstream的mute才会通知给会议的在线成员
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    private int processRequest(JSONObject requestMsg, Result result){
        String client_id = requestMsg.getString("client_id");
        String room_id = requestMsg.getString("room_id");
        String room_domain = requestMsg.getString("room_domain");
        String publish_stream_id = requestMsg.getString("stream_id");
        JSONObject options = requestMsg.getJSONObject("options");
        if(client_id==null||room_id==null||publish_stream_id==null||options==null||room_domain==null){
            log.error("{}: request msg lack params, msg: {}", MCUMuteStreamTask.taskType, requestMsg);
            return AVErrorType.ERR_PARAM_REQUEST;
        }

        Boolean videoMuted = options.getBoolean("videoMuted");
        if(videoMuted==null)
            videoMuted = false;
        Boolean audioMuted = options.getBoolean("audioMuted");
        if(audioMuted==null)
            audioMuted = false;
        result.client_id = client_id;
        result.publish_stream_id = publish_stream_id;
        result.audioMuted = audioMuted;
        result.videoMuted = videoMuted;
        result.room_id = room_id;
        result.room_domain = room_domain;
        if(room_domain.compareTo(domainBean.getSrcDomain())!=0){
            DomainRoute domainRoute = domainBean.getDstDomainRoute(result.room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        MCUMuteStreamTask.taskType, room_domain, requestMsg);
                return AVErrorType.ERR_ROUTE_NOTEXIST;
            }
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            requestMsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", requestMsg);
            return AVErrorType.ERR_NOERROR;
        }

        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }
        result.room_domain = avLogicRoom.getRoom_domain();
        result.avLogicRoom = avLogicRoom;
        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(client_id) == false)
            return AVErrorType.ERR_ROOM_KICK;

        result.client_domain = roomMemInfoMap.get(client_id).getMem_domain();
        //检查是否为publishstream mute操作，若是的话则更新
        PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(publish_stream_id);
        if(publishStreamInfo==null)
            return AVErrorType.ERR_STREAM_SHUTDOWN;

        if(publishStreamInfo.getPublish_clientid().compareTo(client_id)==0){
            result.isPublisher = true;
            publishStreamInfo.setVideoMuted(videoMuted);
            publishStreamInfo.setAudioMuted(audioMuted);

            //更新会议室信息
            if(RedisUtils.hset(redisTemplate, avRoomsKey, avRoomItem, avLogicRoom)==false){
                log.error("redis hset avroominfo failed, hashkey: {}, value: {}", avRoomItem, avLogicRoom);
            }
        }

        //考虑级联订阅的情况，仅在距离发布端或订阅段最近的mcu或emcu处理请求
        //获得处理该发布流的mcu
        String publisher_client_domain = avLogicRoom.getRoom_mems().get(publishStreamInfo.getPublish_clientid()).getMem_domain();
        String maybe_subscriber_client_domain = avLogicRoom.getRoom_mems().get(client_id).getMem_domain();
        Map<String, List<MPServerInfo>> room_mcu_resources = publishStreamInfo.getPublish_stream_resources();
        MPServerInfo avail_mcu = null;
        Iterator<Map.Entry<String, List<MPServerInfo>>> resource_itr = room_mcu_resources.entrySet().iterator();
        while(resource_itr.hasNext()){
            Map.Entry<String, List<MPServerInfo>> entry = resource_itr.next();
            List<MPServerInfo> mpServerInfos = entry.getValue();
            int mpServerInfos_size = mpServerInfos.size();
            for(int mcu_index=0; mcu_index<mpServerInfos_size; mcu_index++){
                MPServerInfo mcu_info = mpServerInfos.get(mcu_index);
                if(result.isPublisher==true){
                    if(publisher_client_domain.compareTo(room_domain)!=0){
                        //在发布终端所在域的emcu处理
                        if(publisher_client_domain.compareTo(mcu_info.getSrc_domain())==0 &&
                                mcu_info.isEmcu()==true){
                            avail_mcu = mcu_info;
                            break;
                        }
                    }else{
                        //在发布终端所在域的mcu处理
                        if(publisher_client_domain.compareTo(mcu_info.getSrc_domain())==0 &&
                                mcu_info.isEmcu()==false){
                            avail_mcu = mcu_info;
                            break;
                        }
                    }
                }else{
                    if(maybe_subscriber_client_domain.compareTo(room_domain)!=0){
                        //在订阅终端所在域的emcu处理
                        if(maybe_subscriber_client_domain.compareTo(mcu_info.getSrc_domain())==0 &&
                                mcu_info.isEmcu()==true){
                            avail_mcu = mcu_info;
                            break;
                        }
                    }else{
                        //在订阅终端所在域的mcu处理
                        if(maybe_subscriber_client_domain.compareTo(mcu_info.getSrc_domain())==0 &&
                                mcu_info.isEmcu()==false){
                            avail_mcu = mcu_info;
                            break;
                        }
                    }
                }
            }
        }

        if(avail_mcu!=null){
            String mcu_domain = avail_mcu.getSrc_domain();
            if(mcu_domain.compareTo(domainBean.getSrcDomain())==0){
                String mcu_id = avail_mcu.getMp_id();
                String mcu_bindkey = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
                log.info("mq send to mcu {}: {}", mcu_bindkey,requestMsg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, requestMsg);
            }else{
                //跨域发送至mcu
                DomainRoute domainRoute = domainBean.getDstDomainRoute(mcu_domain);
                if(domainRoute==null){
                    log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                            MCUMuteStreamTask.taskType, mcu_domain, requestMsg);
                    return AVErrorType.ERR_ROUTE_NOTEXIST;
                }

                JSONObject crossDomainmsg = new JSONObject();
                crossDomainmsg.put("type", CDCrossDomainToMCUTask.taskType);
                crossDomainmsg.put("mcu_id", avail_mcu.getMp_id());
                crossDomainmsg.put("encap_msg", requestMsg);
                List<DomainRoute> new_domain_list = new LinkedList<>();
                new_domain_list.add(domainRoute);
                JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                crossDomainmsg.put("domain_route", domain_array);
                log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
            }
        }

        return AVErrorType.ERR_NOERROR;
    }

    private int sendResponse(int processCode, Result result){
        if(result.room_domain.compareTo(domainBean.getSrcDomain())==0)
            return processCode;

        if(result.client_id.length()>0 && result.client_domain.length()>0){
            JSONObject response_msg = new JSONObject();
            response_msg.put("type", MCUMuteStreamTask.taskResType);
            response_msg.put("retcode", processCode);
            response_msg.put("client_id", result.client_id);
            response_msg.put("stream_id", result.publish_stream_id);
            response_msg.put("room_id", result.room_id);
            JSONObject option_msg = new JSONObject();
            option_msg.put("videoMuted", result.videoMuted);
            option_msg.put("audioMuted", result.audioMuted);
            response_msg.put("options", option_msg);
            if(result.client_domain.compareTo(domainBean.getSrcDomain())==0){
                String client_sendkey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.client_id;
                log.info("mq send to client {}: {}", client_sendkey,response_msg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_sendkey, response_msg);
            }else{
                DomainRoute domainRoute = domainBean.getDstDomainRoute(result.client_domain);
                if(domainRoute==null){
                    log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                            MCUMuteStreamTask.taskType, result.client_domain, response_msg);
                    return processCode;
                }
                JSONObject crossDomainmsg = new JSONObject();
                crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                crossDomainmsg.put("client_id", result.client_id);
                crossDomainmsg.put("encap_msg", response_msg);
                List<DomainRoute> new_domain_list = new LinkedList<>();
                new_domain_list.add(domainRoute);
                JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                crossDomainmsg.put("domain_route", domain_array);
                log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
            }
        }
        return processCode;
    }

    private int sendNotice(int processCode, Result result){
        if(processCode!=AVErrorType.ERR_NOERROR)
            return processCode;

        if(result.isPublisher==false)
            return processCode;

        if(result.room_domain.compareTo(domainBean.getSrcDomain())==0)
            return processCode;

        JSONObject notice_msg = new JSONObject();
        notice_msg.put("type", MCUMuteStreamTask.taskNotType);
        notice_msg.put("publish_stream_id",result.publish_stream_id);
        notice_msg.put("room_id", result.room_id);
        JSONObject option_msg = new JSONObject();
        option_msg.put("videoMuted", result.videoMuted);
        option_msg.put("audioMuted", result.audioMuted);
        notice_msg.put("options", option_msg);
        //获取在会成员
        Iterator<Map.Entry<String, RoomMemInfo>> iterator = result.avLogicRoom.getRoom_mems().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, RoomMemInfo> entry = iterator.next();
            RoomMemInfo notice_mem = entry.getValue();
            String mem_id = notice_mem.getMem_id();
            boolean mem_online = notice_mem.isMem_Online();
            String mem_domain = notice_mem.getMem_domain();
            if(mem_id.compareTo(result.client_id)==0 || mem_online==false)
                continue;
            if(mem_domain.compareTo(domainBean.getSrcDomain())==0){
                String mem_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+mem_id;
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
        return processCode;
    }

    @Override
    public void run() {
        log.info("execute MCUMuteStreamTask at {}", new Date());
        try {
            JSONObject requestMsg = JSON.parseObject(msg);
            int retcode = AVErrorType.ERR_NOERROR;
            Result result = new Result();
            retcode = processRequest(requestMsg,result);
            retcode = sendResponse(retcode,result);
            sendNotice(retcode,result);
        }catch(Exception e){
            e.printStackTrace();
            return;
        }
    }

    class Result{
        String client_id = "";
        String client_domain = "";
        String publish_stream_id = "";
        String room_id = "";
        String room_domain = "";
        AVLogicRoom avLogicRoom;
        boolean videoMuted = false;
        boolean audioMuted = false;
        boolean isPublisher = false;
    }
}
