package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.DomainRoute;
import com.example.demo.po.MPServerInfo;
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

@Component(value= CDSignalMsgSDPTask.taskType)
@Scope("prototype")
public class CDSignalMsgSDPTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDSignalMsgSDPTask.class);
    public final static String taskType = "cascade_signal_sdp";
    public final static String signalMsgType = "signal_message";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        log.info("execute CDSignalMsgSDPTask at {}, msg: {}", new Date(), msg);
        msg = msg.replaceAll("\\\\n", "\n");
        msg = msg.replaceAll("\\\\", "");
        log.warn("after signal sdp msg: {}", msg);
        JSONObject requestMsg = JSON.parseObject(msg);
        String mcu_id = requestMsg.getString("mcu_id");
        String mcu_domain = requestMsg.getString("mcu_domain");
        requestMsg.put("type", CDSignalMsgSDPTask.signalMsgType);
        if(mcu_domain.compareTo(domainBean.getSrcDomain())==0){
            String av_mps_key = MQConstant.REDIS_MPINFO_HASH_KEY;
            String av_mp_hashkey = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
            MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate, av_mps_key, av_mp_hashkey);
            if(mpServerInfo == null){
                log.error("can not get MPServerInfo, msg: {}", msg);
                return;
            }
            log.info("mq send to mcu {}: {}", mpServerInfo.getBinding_key(),requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mpServerInfo.getBinding_key(), requestMsg);
        }else{
            DomainRoute domainRoute = domainBean.getDstDomainRoute(mcu_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        CDCascadeSubscribeTask.taskType, mcu_domain, requestMsg);
                return;
            }
            JSONObject crossDomainmsg = new JSONObject();
            crossDomainmsg.put("type", CDCrossDomainToMCUTask.taskType);
            crossDomainmsg.put("mcu_id", mcu_id);
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
