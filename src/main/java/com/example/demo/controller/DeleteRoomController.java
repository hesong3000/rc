package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.task.RCDeleteRoomTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DeleteRoomController {

    @RequestMapping("/deleteroom")
    @ResponseBody
    private String deleteRoom(){
        JSONObject request_msg = new JSONObject();
        request_msg.put("type", RCDeleteRoomTask.taskType);
        request_msg.put("room_id", "333333");
        request_msg.put("client_id", "client_1");
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, request_msg);
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
