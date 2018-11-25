package com.example.demo.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

public class RCUserExpiredTask implements Runnable{
    private static Logger log = LoggerFactory.getLogger(RCUserExpiredTask.class);
    public final static String taskType = "user_expired_report";
    private String msg;
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public void run() {

    }
}
