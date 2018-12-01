package com.example.demo.task;

public class SimpleTask implements Runnable{
    protected String msg;
    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public void run() {
    }
}
