package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQConstant;
import com.example.demo.task.MCURegisterTask;
import com.example.demo.task.RCUserConnectTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class MCURegisterController {
    @RequestMapping("/mcuregister")
    @ResponseBody
    private String mcuRegister(){
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("type", MCURegisterTask.taskType);
        map.put("mcu_id", "mcu_12345");
        map.put("expired", 0);
        map.put("binding_key", "mcu_12345_binding_key");
        map.put("max_stream_count", 300);
        map.put("reserve_stream_count", 20);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, JSON.toJSON(map));
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
