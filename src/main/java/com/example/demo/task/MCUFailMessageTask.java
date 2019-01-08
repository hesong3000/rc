package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVLogicRoom;
import com.example.demo.po.AVStreamInfo;
import com.example.demo.po.AVUserInfo;
import com.example.demo.po.DomainRoute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@Component(value= MCUFailMessageTask.taskType)
@Scope("prototype")
public class MCUFailMessageTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCUFailMessageTask.class);
    public final static String taskType = "msg_fail_response";
    //addpublisher过程中的rollback
    public final static String taskPublishRollback = "remove_publisher";
    //addsubscriber过程中的rollback
    public final static String taskSubscribRollback = "remove_subscriber";
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
            2、向MCU发送Rollback请求，目前只是removePublish和removeSubscriber两种消息
            3、向客户端发送fail_msg
            4、在Redis中删除AVStreamInfo键
         */
        log.info("execute MCUFailMessageTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        String mcu_id = requestMsg.getString("mcu_id");
        JSONObject fail_msg = requestMsg.getJSONObject("msg");
        if(client_id==null||mcu_id==null||fail_msg==null){
            log.error("MCUFailMessageTask msg invaild, msg: {}",msg);
            return;
        }
        String stream_id = fail_msg.getString("stream_id");
        String sub_type = fail_msg.getString("sub_type");
        String reason = fail_msg.getString("reason");
        if(stream_id==null||sub_type==null||reason==null){
            log.error("MCUFailMessageTask failmsg detail lack content, msg: {}",msg);
            return;
        }
        log.warn("mcu process media request failed, failed msg: {}", msg);

        //检查AVStreamInfo是否存在，不存在为代码BUG
        String avstream_key = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avstream_key);
        if(avStreamInfo==null){
            log.error("redis get avstream failed, key: {}", avstream_key);
            return;
        }

        //获取逻辑会议室
        String room_id = avStreamInfo.getRoom_id();
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            log.error("{} can not find avlogicroom", MCUFailMessageTask.taskType);
            return;
        }

        //对MCU进行rollback操作
        int retcode = AVErrorType.ERR_NOERROR;
        JSONObject rollback_msg = new JSONObject();
        Boolean isPublisher = false;
        if(avStreamInfo.getPublisher_id().compareTo(client_id)==0) {
            retcode = AVErrorType.ERR_STREAM_PUBLISH;
            //发送removePublish消息
            rollback_msg.put("type", MCUFailMessageTask.taskPublishRollback);
            rollback_msg.put("client_id", client_id);
            rollback_msg.put("stream_id", stream_id);
            isPublisher = true;
        }
        else {
            retcode = AVErrorType.ERR_STREAM_SUBSCRIBE;
            //发送removeSubscriber消息
            rollback_msg.put("type", MCUFailMessageTask.taskSubscribRollback);
            rollback_msg.put("client_id", client_id);
            rollback_msg.put("publish_stream_id", stream_id);
            isPublisher = false;
        }

        //回滚操作
        rollback_msg.put("room_id", avLogicRoom.getRoom_id());
        log.info("mq send RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,rollback_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, rollback_msg);

        //检查客户端是否存活，若存活则将错误信息发送至客户端
        String avUserKey = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate, avUserKey);
        JSONObject client_fail_msg = new JSONObject();
        client_fail_msg.put("type", MCUFailMessageTask.taskType);
        client_fail_msg.put("client_id", client_id);
        client_fail_msg.put("stream_id", stream_id);
        client_fail_msg.put("is_publisher", isPublisher);
        client_fail_msg.put("retcode", retcode);
        if(avUserInfo!=null){
            String client_domain = avUserInfo.getSrc_domain();
            if(client_domain.compareTo(domainBean.getSrcDomain())==0){
                String client_bindkey = avUserInfo.getBinding_key();
                log.info("mq send to client {}: {}", client_bindkey,client_fail_msg);
                rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindkey, client_fail_msg);
            }
            else{
                DomainRoute domainRoute = domainBean.getDstDomainRoute(client_domain);
                if(domainRoute==null){
                    log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                            MCUFailMessageTask.taskType, client_domain, client_fail_msg);
                    return;
                }
                JSONObject crossDomainmsg = new JSONObject();
                crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
                crossDomainmsg.put("client_id", client_id);
                crossDomainmsg.put("encap_msg", client_fail_msg);
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
