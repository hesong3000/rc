package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.MQConstant;
import com.example.demo.po.AVStreamInfo;
import com.example.demo.po.AVUserInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component(value= CDCrossDomainMsgTask.taskType)
@Scope("prototype")
public class CDCrossDomainMsgTask extends SimpleTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(CDCrossDomainMsgTask.class);
    public final static String taskType = "cross_domain_msg";
    public final static String getRoomStreamTaskType = "get_room_streams_response";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void run() {
         /*
          1、判断用户是否登录在本域，且是否在线
          2、消息解封装
          2、透传消息给用户
         */
        log.info("execute CDCrossDomainMsgTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        String client_id = requestMsg.getString("client_id");
        String userKey = MQConstant.REDIS_USER_KEY_PREFIX+client_id;
        AVUserInfo avUserInfo = (AVUserInfo)RedisUtils.get(redisTemplate,userKey);
        JSONObject encap_msg = requestMsg.getJSONObject("encap_msg");
        String taskType = encap_msg.getString("type");
        if(taskType.compareTo(CDCrossDomainMsgTask.getRoomStreamTaskType)==0){
            //跨域获取会议室发布流，需要在本域存储
            String room_id = encap_msg.getString("room_id");
            String room_domain = encap_msg.getString("room_domain");
            Integer retcode = encap_msg.getInteger("retcode");
            if(retcode!=null && retcode== AVErrorType.ERR_NOERROR){
                JSONArray streams = encap_msg.getJSONArray("streams");
                int room_stream_size = streams.size();
                for(int index=0;index<room_stream_size;index++){
                    JSONObject streamInfo = streams.getJSONObject(index);
                    String publish_streamid = streamInfo.getString("publish_streamid");
                    String publish_clientid = streamInfo.getString("publish_clientid");
                    Boolean screencast = streamInfo.getBoolean("screencast");
                    Boolean audioMuted = streamInfo.getBoolean("audioMuted");
                    Boolean videoMuted = streamInfo.getBoolean("videoMuted");
                    AVStreamInfo avStreamInfo = new AVStreamInfo();
                    avStreamInfo.setRoom_id(room_id);
                    avStreamInfo.setRoom_domain(room_domain);
                    avStreamInfo.setStream_id(publish_streamid);
                    avStreamInfo.setScreencast(screencast);
                    avStreamInfo.setVideoMuted(videoMuted);
                    avStreamInfo.setAudioMuted(audioMuted);
                    avStreamInfo.setPublisher_id(publish_clientid);
                    avStreamInfo.setPublish(true);
                    String avStreamKey = MQConstant.REDIS_STREAM_KEY_PREFIX+publish_streamid;
                    if(!RedisUtils.set(redisTemplate,avStreamKey,avStreamInfo)){
                        log.error("redis set failed, key: {}, value: {}", avStreamKey, avStreamInfo);
                        continue;
                    }
                }
            }
        }

        if(avUserInfo!=null){
            log.info("mq send to client {}: {}", avUserInfo.getBinding_key(),encap_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, avUserInfo.getBinding_key(),encap_msg);
        }else{
            log.info("send CDCrossDomainMsgTask ignore, while user: {} is offline", client_id);
        }
    }
}
