package com.example.demo.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.MQConstant;
import com.example.demo.task.MCUMuteStreamTask;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MuteStreamController {

    @RequestMapping("/mutestream")
    @ResponseBody
    private String muteStream(){
        JSONObject request_msg = new JSONObject();
        request_msg.put("type", MCUMuteStreamTask.taskType);
        request_msg.put("client_id", "client_2");
        request_msg.put("stream_id", "stream_client_1_publish");
        request_msg.put("room_id", "333333");
        JSONObject option_msg = new JSONObject();
        option_msg.put("videoMuted", true);
        option_msg.put("audioMuted", false);
        request_msg.put("options", option_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, MQConstant.MQ_RC_BINDING_KEY, request_msg);
        return "OK";
    }

    @Autowired
    private AmqpTemplate rabbitTemplate;
}
