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
    @Autowired
    DomainDefineBean domainBean;

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
        String room_domain = requestMsg.getString("room_domain");
        String stream_id = requestMsg.getString("stream_id");
        if(client_id==null||room_id==null||stream_id==null||room_domain==null){
            log.error("remove_publisher msg lack params, msg: {}", msg);
            return;
        }

        if(room_domain.compareTo(domainBean.getSrcDomain())!=0){
            //跨域发送至room域
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        MCURemovePublishTask.taskType, room_domain, requestMsg);
                return;
            }
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            requestMsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", requestMsg);
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
                Map<String, List<MPServerInfo>> publishStream_resources = publishStreamInfo.getPublish_stream_resources();
                Iterator<Map.Entry<String, List<MPServerInfo>>> resource_iter = publishStream_resources.entrySet().iterator();
                while(resource_iter.hasNext()){
                    Map.Entry<String, List<MPServerInfo>> entry = resource_iter.next();
                    List<MPServerInfo> mpServerInfos = entry.getValue();
                    int mpServerInfos_size = mpServerInfos.size();
                    for(int mcu_index=0;mcu_index<mpServerInfos_size;mcu_index++) {
                        MPServerInfo mcu_info = mpServerInfos.get(mcu_index);
                        if(mcu_info.getSrc_domain().compareTo(domainBean.getSrcDomain())==0){
                            String mcu_id = mcu_info.getMp_id();
                            String mcu_key = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
                            log.info("mq send to mcu {}: {}", mcu_key,requestMsg);
                            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_key, requestMsg);
                            //更新hash键AV_MPs的mcu使用率
                            String mcu_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
                            MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, MQConstant.REDIS_MPINFO_HASH_KEY,mcu_hashkey);
                            if(mpServerInfo!=null){
                                mpServerInfo.clearMcuUseResourceOnRemove(room_id,stream_id);
                                if(RedisUtils.hset(redisTemplate, MQConstant.REDIS_MPINFO_HASH_KEY, mcu_hashkey, mpServerInfo)==false){
                                    log.error("redis hset failed, key: {}, hashkey: {}, value: {}",
                                            MQConstant.REDIS_MPINFO_HASH_KEY,
                                            mcu_hashkey,
                                            mpServerInfo);
                                }
                            }
                        }else{
                            //跨域发送至mcu
                            DomainRoute domainRoute = domainBean.getDstDomainRoute(mcu_info.getSrc_domain());
                            if(domainRoute==null){
                                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                                        MCUMuteStreamTask.taskType, mcu_info.getSrc_domain(), requestMsg);
                                return;
                            }

                            JSONObject crossDomainmsg = new JSONObject();
                            crossDomainmsg.put("type", CDCrossDomainToMCUTask.taskType);
                            crossDomainmsg.put("mcu_id", mcu_info.getMp_id());
                            crossDomainmsg.put("encap_msg", requestMsg);
                            List<DomainRoute> new_domain_list = new LinkedList<>();
                            new_domain_list.add(domainRoute);
                            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                            crossDomainmsg.put("domain_route", domain_array);
                            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
                        }
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
                JSONObject stream_remove_notice = new JSONObject();
                stream_remove_notice.put("type", MCURemovePublishTask.taskNotType);
                stream_remove_notice.put("client_id", client_id);
                stream_remove_notice.put("room_id", avLogicRoom.getRoom_id());
                stream_remove_notice.put("stream_id", stream_id);
                Iterator<Map.Entry<String, RoomMemInfo>> iterator = avLogicRoom.getRoom_mems().entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, RoomMemInfo> entry = iterator.next();
                    RoomMemInfo roomMemInfo = entry.getValue();
                    if(roomMemInfo.getMem_id().compareTo(client_id)==0)
                        continue;

                    if(roomMemInfo.isMem_Online()==false)
                        continue;

                    if(roomMemInfo.getMem_domain().compareTo(domainBean.getSrcDomain())==0){
                        String mem_routingkey = MQConstant.MQ_CLIENT_KEY_PREFIX+roomMemInfo.getMem_id();
                        log.info("mq send notice {}: {}",mem_routingkey,stream_remove_notice);
                        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mem_routingkey, stream_remove_notice);
                    }else{
                        DomainRoute domainRoute = domainBean.getDstDomainRoute(roomMemInfo.getMem_domain());
                        if(domainRoute!=null){
                            JSONObject crossDomainmsg = new JSONObject();
                            crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                            crossDomainmsg.put("client_id", roomMemInfo.getMem_id());
                            crossDomainmsg.put("encap_msg", stream_remove_notice);
                            List<DomainRoute> new_domain_list = new LinkedList<>();
                            new_domain_list.add(domainRoute);
                            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
                            crossDomainmsg.put("domain_route", domain_array);
                            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
                            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
                        }
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
