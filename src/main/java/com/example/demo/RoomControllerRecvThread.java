package com.example.demo;

import com.alibaba.fastjson.JSON;
import com.example.demo.config.MQMessageType;
import com.example.demo.task.RCCreateRoomTask;
import com.example.demo.task.RCUserConnectTask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component(value="roomControllerRecvThread")
public class RoomControllerRecvThread extends Thread{
    private ExecutorService executorService;
    @Autowired
    private RoomMsgHolder roomMsgHolder;
    private Map<String,Class> tasks = new HashMap<>();
    @Resource
    private RedisTemplate<String, Object> redisTemplate;
    public RoomControllerRecvThread() {
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);
        //tasks.put("create_room", RCCreateRoomTask.class);
        tasks.put(RCUserConnectTask.taskType, RCUserConnectTask.class);
    }

    @Override
    public void run() {
        while(true){
            String msg = roomMsgHolder.popMsg();
            System.out.println("recv rabbitmq msg: "+msg);
            Map<String,Object> map = (Map<String, Object>) JSON.parse(msg);
            String type = (String)map.get("type");
            try {
                if(!tasks.containsKey(type))
                    continue;
                Class<?> c = tasks.get(type);
                Constructor c1=c.getDeclaredConstructor(new Class[]{String.class, RedisTemplate.class});
                c1.setAccessible(true);
                executorService.submit((Runnable)c1.newInstance(new Object[]{msg, redisTemplate}));
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
