package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.task.RCUserConnectTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import java.util.HashMap;
import java.util.Map;

@RestController
public class ConnectUserController {
    @RequestMapping("/connectuser")
    @ResponseBody
    public String connectUser(){
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("type", RCUserConnectTask.taskType);
        map.put("client_id", "client_2");
        map.put("client_name", "user_2");
        map.put("binding_key", "client_2_binding_key");
        map.put("expired", 0);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map));

        Map<String, Object> map_1 = new HashMap<String, Object>();
        map_1.put("type", RCUserConnectTask.taskType);
        map_1.put("client_id", "client_1");
        map_1.put("client_name", "user_1");
        map_1.put("binding_key", "client_1_binding_key");
        map_1.put("expired", 0);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map_1));
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
