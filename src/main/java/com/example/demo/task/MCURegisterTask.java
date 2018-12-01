package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.po.MPServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component(value=MCURegisterTask.taskType)
@Scope("prototype")
public class MCURegisterTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCURegisterTask.class);
    public final static String taskType = "mcu_register_request";
    public final static String taskResType = "mcu_register_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    @Transactional
    public void run() {
        log.info("execute MCURegisterTask at {}", new Date());
        JSONObject jsonObject = JSON.parseObject(msg);
        String mcu_id = jsonObject.getString("mcu_id");
        Integer expired = jsonObject.getInteger("expired");
        Integer max_stream_count = jsonObject.getInteger("max_stream_count");
        String binding_key = jsonObject.getString("binding_key");
        Integer reserve_stream_count = jsonObject.getInteger("reserve_stream_count");
        if(mcu_id==null||expired==null||max_stream_count==null||binding_key==null||reserve_stream_count==null){
            log.error("{} params invalid, msg: {}",MCURegisterTask.taskType,msg);
            return;
        }

        String mcu_key = MQConstant.REDIS_MP_KEY_PREFIX+mcu_id;
        //更新普通键AV_MP：[MPID]的过期时间
        if(!RedisUtils.set(redisTemplate,mcu_key,mcu_id,expired)){
            log.error("redis set failed, key: {}, value: {}",mcu_key,mcu_id);
            return;
        }

        //更新或增加MPServer信息到hash键： AV_MPs
        String avMPs_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        String avMP_item = MQConstant.REDIS_MP_ROOM_KEY_PREFIX+mcu_id;
        MPServerInfo mpServerInfo = (MPServerInfo)RedisUtils.hget(redisTemplate,avMPs_key,avMP_item);
        if(mpServerInfo==null) {
            mpServerInfo = new MPServerInfo();
            mpServerInfo.setBinding_key(binding_key);
            mpServerInfo.setMp_id(mcu_id);
            mpServerInfo.setMax_stream_count(max_stream_count);
            mpServerInfo.setReserve_stream_count(reserve_stream_count);
            mpServerInfo.setUserd_stream_count(0);
        }
        if(!RedisUtils.hset(redisTemplate,avMPs_key,avMP_item,mpServerInfo)){
            log.error("redis hset failed, key: {}, item: {}, value: {}",avMPs_key,avMP_item,mpServerInfo.toString());
            return;
        }

        //发送response
        Map<String, String> map_res = new HashMap<String, String>();
        map_res.put("type", MCURegisterTask.taskResType);
        map_res.put("mcu_id", mcu_id);
        log.info("mq send response {}: {}", binding_key,JSON.toJSONString(map_res));
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, binding_key, JSON.toJSON(map_res));
    }
}
