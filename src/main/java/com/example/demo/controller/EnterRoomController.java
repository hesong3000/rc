package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.task.RCEnterRoomTask;
import com.example.demo.task.RCUserConnectTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class EnterRoomController {
    @RequestMapping("/enterroom")
    @ResponseBody
    public String enternRoom(){
        //for(int i = 0; i<99; i++) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("type", RCEnterRoomTask.taskType);
        map.put("room_id", "333333");
        map.put("client_id", "client_3");
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map));

        Map<String, String> map_2 = new HashMap<String, String>();
        map_2.put("type", RCEnterRoomTask.taskType);
        map_2.put("room_id", "333333");
        map_2.put("client_id", "client_2");
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map_2));
        //}
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
