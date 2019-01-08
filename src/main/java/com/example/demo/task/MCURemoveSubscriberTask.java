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

@Component(value= MCURemoveSubscriberTask.taskType)
@Scope("prototype")
public class MCURemoveSubscriberTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCURemoveSubscriberTask.class);
    public final static String taskType = "remove_subscriber";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        /*
            1、removeSubscriber过程只做处理，不做失败回复，因为客户端在removeSubscriber前已手动关闭媒体流，
               信令接收通道也有可能关闭，此时如果回复可能会造成客户端处理崩溃
            2、向MCU发送removeSubscriber请求
            3、更新逻辑会议室中有关媒体流相关的信息
         */
        log.info("execute MCURemoveSubscriberTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        String room_id = requestMsg.getString("room_id");
        String room_domain = requestMsg.getString("room_domain");
        String publish_stream_id = requestMsg.getString("publish_stream_id");
        if(client_id==null||room_id==null||publish_stream_id==null||room_domain==null){
            log.error("remove_publisher msg lack params, msg: {}", msg);
            return;
        }

        if(room_domain.compareTo(domainBean.getSrcDomain())!=0){
            //跨域发送至room域
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        MCURemoveSubscriberTask.taskType, room_domain, requestMsg);
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

        //向MCU发送removeSubscriber请求
        if(avLogicRoom.getPublish_streams().containsKey(publish_stream_id)==true){
            PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(publish_stream_id);
            //获取距离订阅端最近的mcu处理请求
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

            if(avail_mcu!=null){
                String mcu_domain = avail_mcu.getSrc_domain();
                if(mcu_domain.compareTo(domainBean.getSrcDomain())==0){
                    String mcu_id = avail_mcu.getMp_id();
                    String mcu_bindkey = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
                    log.info("mq send to mcu {}: {}", mcu_bindkey,requestMsg);
                    rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, requestMsg);
                    //更新mcu资源
                    //查询处理发布流的mcu信息，无法查询到是BUG
                    String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
                    String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
                    MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
                    if(mpServerInfo != null){
                        //更新MCU使用率信息
                        //若publishstream在本域处理，则更新mcu使用率信息
                        //流发布成功，占用一路mcu资源
                        mpServerInfo.addMcuUseResource(room_id, 1);
                        //将MCU的更新信息存储至Redis
                        if(RedisUtils.hset(redisTemplate, av_mps_key, av_mp_hashkey, mpServerInfo)==false){
                            log.error("redis hset mpserver info failed, key: {} hashket: {}, value: {}",
                                    av_mps_key, av_mp_hashkey, mpServerInfo);
                        }
                    }
                }else{
                    //跨域发送至mcu
                    DomainRoute domainRoute = domainBean.getDstDomainRoute(mcu_domain);
                    if(domainRoute==null){
                        log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                                MCUMuteStreamTask.taskType, mcu_domain, requestMsg);
                        return;
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

            //更新publish_stream信息
            publishStreamInfo.getSubscribers().remove(client_id);
            //更新订阅者的订阅流信息
            avLogicRoom.getRoom_mems().get(client_id).getSubscribe_streams().remove(publish_stream_id);
        }

        //更新会议室信息
        if(RedisUtils.hset(redisTemplate,avRoomsKey,avRoomItem,avLogicRoom)==false){
            log.warn("redis hset failed, key: {}, hashkey: {}, value: {}",avRoomsKey,avRoomItem,avLogicRoom);
        }
    }
}
