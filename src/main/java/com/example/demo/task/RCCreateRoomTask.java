package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.example.demo.po.CreateRoomMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RCCreateRoomTask implements Runnable {
    private String msg;
    private static Logger log = LoggerFactory.getLogger(RCCreateRoomTask.class);
    RCCreateRoomTask(String msg){
        this.msg = msg;
    }
    @Override
    public void run() {
        CreateRoomMsg objBean = JSON.parseObject(msg,CreateRoomMsg.class);

    }
}
