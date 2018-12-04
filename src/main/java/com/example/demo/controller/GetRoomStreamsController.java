package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.task.GetRoomStreamsRequest;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GetRoomStreamsController {
    @RequestMapping("/getroomstreams")
    @ResponseBody
    private String getroomstreams(){
        JSONObject request_msg = new JSONObject();
        request_msg.put("type", GetRoomStreamsRequest.taskType);
        request_msg.put("client_id", "client_2");
        request_msg.put("room_id", "333333");
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, request_msg);
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
