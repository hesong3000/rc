package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.po.CreateRoomMsg;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RestController
public class CreateRoomController {
    @RequestMapping("/createroom")
    @ResponseBody
    public String createRoom(){
        CreateRoomMsg createRoomMsg = new CreateRoomMsg();
        createRoomMsg.setCreator_id("11111");
        createRoomMsg.setRoom_id("22222");
        createRoomMsg.setRoom_name("room");
        createRoomMsg.setType("create_room");
        List<Map<String, String>> user_list = new LinkedList<Map<String, String>>();
        Map<String, String> map = new HashMap<String, String>();
        map.put("client_id", "client_1");
        map.put("user_name", "user_1");
        user_list.add(map);
        createRoomMsg.setUser_list(user_list);
        rabbitTemplate.convertAndSend(MQConstant.EXCHANGE, MQConstant.RC_BINDING_KEY,JSON.toJSONString(createRoomMsg));
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
