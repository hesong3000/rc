package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.po.CreateRoomMsg;
import com.example.demo.task.RCCreateRoomTask;
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
        //for(int i = 0; i<99; i++) {
            CreateRoomMsg createRoomMsg = new CreateRoomMsg();
            createRoomMsg.setCreator_id("client_1");
            createRoomMsg.setRoom_id("333333");
            createRoomMsg.setRoom_name("room2");
            createRoomMsg.setType(RCCreateRoomTask.taskType);
            List<Map<String, String>> user_list = new LinkedList<Map<String, String>>();
            Map<String, String> user1 = new HashMap<String, String>();
            user1.put("mem_id", "client_1");
            user1.put("mem_name", "user_1");
            user_list.add(user1);
            Map<String, String> user2 = new HashMap<String, String>();
            user2.put("mem_id", "client_2");
            user2.put("mem_name", "user_2");
            user_list.add(user2);
            Map<String, String> user3 = new HashMap<String, String>();
            user3.put("mem_id", "client_3");
            user3.put("mem_name", "user_3");
            user_list.add(user3);
            createRoomMsg.setMem_list(user_list);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(createRoomMsg));
        //}
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
