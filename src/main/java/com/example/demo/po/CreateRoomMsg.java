package com.example.demo.po;

import java.util.List;
import java.util.Map;

public class CreateRoomMsg {
    String type;
    String creator_id;
    String room_id;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCreator_id() {
        return creator_id;
    }

    public void setCreator_id(String creator_id) {
        this.creator_id = creator_id;
    }

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

    public List<Map<String, String>> getUser_list() {
        return user_list;
    }

    public void setUser_list(List<Map<String, String>> user_list) {
        this.user_list = user_list;
    }

    String room_name;
    List<Map<String,String>> user_list;
}
