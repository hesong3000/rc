package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.task.MCUAddPublishTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class AddPublisherController {
    @RequestMapping("/addpublisher")
    @ResponseBody
    public String addPublisher(){
        JSONObject request_msg = new JSONObject();
        request_msg.put("type", MCUAddPublishTask.taskType);
        request_msg.put("room_id", "222222");
        request_msg.put("client_id","client_1");
        request_msg.put("stream_id","stream_"+ UUID.randomUUID().toString());
        request_msg.put("client_bindkey","client_1_bindkey");
        JSONObject option_msg = new JSONObject();
        option_msg.put("screencast", false);
        option_msg.put("audioMuted", true);
        option_msg.put("videoMuted", false);
        request_msg.put("options", option_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, request_msg);
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
