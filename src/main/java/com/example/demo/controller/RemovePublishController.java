package com.example.demo.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.task.MCURemovePublishTask;
import com.example.demo.task.MCURemoveSubscriberTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RemovePublishController {

    @RequestMapping("/removepublisher")
    @ResponseBody
    private String removePublisher(){
        JSONObject request_msg = new JSONObject();
        request_msg.put("type", MCURemovePublishTask.taskType);
        request_msg.put("client_id", "client_1");
        request_msg.put("room_id", "333333");
        request_msg.put("stream_id", "stream_client_1_publish");
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, request_msg);
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
