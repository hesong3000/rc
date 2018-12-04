package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.task.RCGetRoomListTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class GetRoomListController {

    @RequestMapping("/getroomlist")
    @ResponseBody
    public String getRoomList(){
        Map<String, String> map = new HashMap<String, String>();
        map.put("type", RCGetRoomListTask.taskType);
        map.put("client_id", "client_1");
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map));
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
