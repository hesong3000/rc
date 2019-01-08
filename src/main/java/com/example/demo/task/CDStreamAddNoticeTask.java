package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVStreamInfo;
import com.example.demo.po.AVUserInfo;
import com.example.demo.po.MPServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component(value= CDStreamAddNoticeTask.taskType)
@Scope("prototype")
public class CDStreamAddNoticeTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDStreamAddNoticeTask.class);
    public final static String taskType = "crossdomain_stream_add_notice";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Override
    public void run() {
        log.info("execute CDStreamAddNoticeTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String room_domain = requestMsg.getString("room_domain");
        String notice_client_id = requestMsg.getString("notice_client_id");
        JSONObject notice_msg = requestMsg.getJSONObject("notice_msg");
        String room_id = notice_msg.getString("room_id");
        String publisher_clientId = notice_msg.getString("client_id");
        String publisher_streamId = notice_msg.getString("stream_id");
        JSONObject option_msg = notice_msg.getJSONObject("options");
        Boolean has_video = option_msg.getBoolean("video");
        Boolean has_audio = option_msg.getBoolean("audio");
        Boolean is_screencast = option_msg.getBoolean("screencast");
        AVStreamInfo avStreamInfo = new AVStreamInfo();
        avStreamInfo.setStream_id(publisher_streamId);
        avStreamInfo.setPublisher_id(publisher_clientId);
        avStreamInfo.setRoom_id(room_id);
        avStreamInfo.setRoom_domain(room_domain);
        avStreamInfo.setAudioMuted(!has_audio);
        avStreamInfo.setVideoMuted(!has_video);
        avStreamInfo.setScreencast(is_screencast);
        String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+publisher_streamId;
        //在外域增加发布流的存储，用于后续订阅使用
        if(!RedisUtils.set(redisTemplate,avStreamKey,avStreamInfo)){
            log.error("redis set failed, key: {}, value: {}", avStreamKey, avStreamInfo);
            return;
        }

        String avUserkey = MQConstant.REDIS_USER_KEY_PREFIX+notice_client_id;
        AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate, avUserkey);
        if(avUserInfo==null)
            return;

        //将stream_add_notice通知发送至终端
        log.info("mq send notice {}: {}",avUserInfo.getBinding_key(),notice_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, avUserInfo.getBinding_key(), notice_msg);
    }
}
