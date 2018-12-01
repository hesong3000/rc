package com.example.demo.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component(value=RCMPExpiredTask.taskType)
@Scope("prototype")
public class RCMPExpiredTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RCMPExpiredTask.class);
    public final static String taskType = "mpserver_expired_report";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    @Transactional
    public void run(){
        log.info("execute RCMPExpiredTask");
    }
}
