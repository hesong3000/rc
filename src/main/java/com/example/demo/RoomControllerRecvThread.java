package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.task.SimpleTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component(value="roomControllerRecvThread")
public class RoomControllerRecvThread extends Thread implements ApplicationContextAware {
    private ExecutorService executorService;
    private static Logger log = LoggerFactory.getLogger(RoomControllerRecvThread.class);
    private static ApplicationContext context = null;
    @Autowired
    private RoomMsgHolder roomMsgHolder;
    @Resource
    private RedisTemplate<String, Object> redisTemplate;
    public RoomControllerRecvThread() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);
    }

    @Override
    public void run() {
        while(true){
            try {
                String msg = roomMsgHolder.popMsg();
                JSONObject jsonObject = JSON.parseObject(msg);
                String type = jsonObject.getString("type");
                log.info("recv {}, msg: {}", type, msg);
                SimpleTask simpleTask = (SimpleTask)context.getBean(type);
                simpleTask.setMsg(msg);
                executorService.submit(simpleTask);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        executorService.shutdown();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }
}
