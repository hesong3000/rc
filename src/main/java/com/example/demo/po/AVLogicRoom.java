package com.example.demo.po;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AVLogicRoom implements Serializable {
    private String room_id = "";
    private String room_name = "";
    private String creator_id = "";
    private Date create_time = new Date();
    private Map<String, RoomMemInfo> room_mems = new HashMap<>();
    private Map<String, PublishStreamInfo> publish_streams = new HashMap<>();
    private Map<String, CascadeStreamInfo> cascade_streams = new HashMap<>();

    public String getRoom_id() {
        return room_id;
    }

    public void setRoom_id(String room_id) {
        this.room_id = room_id;
    }

    public String getRoom_name() {
        return room_name;
    }

    public void setRoom_name(String room_name) {
        this.room_name = room_name;
    }

    public String getCreator_id() {
        return creator_id;
    }

    public void setCreator_id(String creator_id) {
        this.creator_id = creator_id;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Map<String, RoomMemInfo> getRoom_mems() {
        return room_mems;
    }

    public void setRoom_mems(Map<String, RoomMemInfo> room_mems) {
        this.room_mems = room_mems;
    }

    public Map<String, PublishStreamInfo> getPublish_streams() {
        return publish_streams;
    }

    public void setPublish_streams(Map<String, PublishStreamInfo> publish_streams) {
        this.publish_streams = publish_streams;
    }

    public Map<String, CascadeStreamInfo> getCascade_streams() {
        return cascade_streams;
    }

    public void setCascade_streams(Map<String, CascadeStreamInfo> cascade_streams) {
        this.cascade_streams = cascade_streams;
    }
}
