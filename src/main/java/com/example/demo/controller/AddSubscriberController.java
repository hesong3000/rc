package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.task.MCUAddSubscriberTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AddSubscriberController {

    @RequestMapping("/addsubscriber")
    @ResponseBody
    private String addSubscriber(){
        JSONObject request_msg = new JSONObject();
        request_msg.put("type", MCUAddSubscriberTask.taskType);
        request_msg.put("client_id", "client_2");
        request_msg.put("publish_stream_id", "stream_client_1_publish");
        request_msg.put("stream_id", "subscribe_client_1_publish");
        request_msg.put("room_id", "333333");
        request_msg.put("options", new JSONObject());
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, request_msg);
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
