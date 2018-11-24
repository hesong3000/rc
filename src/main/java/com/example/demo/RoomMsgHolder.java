package com.example.demo;

import org.springframework.stereotype.Component;

import java.util.concurrent.LinkedBlockingQueue;

@Component
public class RoomMsgHolder {
    private static LinkedBlockingQueue<String> concurrentLinkedQueue = new LinkedBlockingQueue<String>();

    public String popMsg(){
        String msg = "";
        try {
            msg = concurrentLinkedQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return msg;
    }

    public void pushMsg(String msg){
        try {
            concurrentLinkedQueue.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
