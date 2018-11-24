package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.config.MQMessageType;
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
        Map<String, String> map = new HashMap<String, String>();
        map.put("type", MQMessageType.type_amqp_connect_request);
        map.put("client_id", "123456");
        map.put("client_name", "wk");
        map.put("binding_key", "wk_binding_key");
        rabbitTemplate.convertAndSend(MQConstant.EXCHANGE, MQConstant.RC_BINDING_KEY, JSON.toJSONString(map));
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
