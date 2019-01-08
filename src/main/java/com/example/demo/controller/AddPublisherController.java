package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.task.MCUAddPublishTask;
import com.example.demo.task.RedisUtils;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
public class AddPublisherController {
    @RequestMapping("/addpublisher")
    @ResponseBody
    public String addPublisher(){
        JSONObject request_msg = new JSONObject();
        request_msg.put("type", MCUAddPublishTask.taskType);
        request_msg.put("room_id", "333333");
        request_msg.put("client_id","client_1");
        request_msg.put("stream_id","stream_client_1_publish");
        JSONObject option_msg = new JSONObject();
        option_msg.put("screencast", false);
        option_msg.put("audioMuted", false);
        option_msg.put("videoMuted", false);
        request_msg.put("options", option_msg);

        String avMPs_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        Map<Object,Object> av_mps = RedisUtils.hmget(redisTemplate,avMPs_key);
        int av_mps_count = av_mps.size();

        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, request_msg);
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
}
