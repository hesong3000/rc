package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVStreamInfo;
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

@Component(value= CDCascadeSubscribeFailTask.taskType)
@Scope("prototype")
public class CDCascadeSubscribeFailTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDCascadeSubscribeFailTask.class);
    public final static String taskType = "cascade_subscriber_fail";
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    DomainDefineBean domainBean;

    @Override
    public void run() {
        log.info("execute CDCascadeSubscribeFailTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String stream_id = requestMsg.getString("stream_id");
        //查找发布流信息
        String avstream_key = MQConstant.REDIS_STREAM_KEY_PREFIX+stream_id;
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avstream_key);
        if(avStreamInfo==null){
            log.error("{} redis get avstream failed, key: {}", CDCascadeSubscribeFailTask.taskType, avstream_key);
            return;
        }

        String room_id = avStreamInfo.getRoom_id();
        String room_domain = avStreamInfo.getRoom_domain();
        requestMsg.put("room_id", room_id);
        requestMsg.put("room_domain", room_domain);
        //判断room是否在本域
        if(room_domain.compareTo(domainBean.getSrcDomain())==0){
            //暂时先发送至客户端，需要清理资源
            log.warn("cascade subscriber failed, msg: {}", requestMsg);
            JSONObject detail_msg  = requestMsg.getJSONObject("detail_msg");
            log.info("mq send RC {}: {}",MQConstant.MQ_RC_BINDING_KEY,detail_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, detail_msg);
        }else{
            //跨域发布到room所在域
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        CDCascadeSubscribeReadyTask.taskType, room_domain, requestMsg);
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
    }
}
