package com.example.demo.po;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RoomMemInfo implements Serializable {
    private Boolean mem_online = false;
    private String mem_id = "";
    private String mem_name = "";
    private Map<String, String> publish_streams = new HashMap<>();
    private Map<String, String> subscribe_streams = new HashMap<>();

    public Boolean isMem_Online() {
        return mem_online;
    }

    public void setMem_Online(Boolean mem_online) {
        this.mem_online = mem_online;
    }

    public String getMem_id() {
        return mem_id;
    }

    public void setMem_id(String mem_id) {
        this.mem_id = mem_id;
    }

    public String getMem_name() {
        return mem_name;
    }

    public void setMem_name(String mem_name) {
        this.mem_name = mem_name;
    }

    public Map<String, String> getPublish_streams() {
        return publish_streams;
    }

    public void setPublish_streams(Map<String, String> publish_streams) {
        this.publish_streams = publish_streams;
    }

    public Map<String, String> getSubscribe_streams() {
        return subscribe_streams;
    }

    public void setSubscribe_streams(Map<String, String> subscribe_streams) {
        this.subscribe_streams = subscribe_streams;
    }
}
