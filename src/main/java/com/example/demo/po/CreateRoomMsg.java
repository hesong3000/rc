package com.example.demo.po;

import java.util.List;
import java.util.Map;

public class CreateRoomMsg {
    String type;
    String creator_id;
    String room_id;
    String room_name;

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

    public List<Map<String, String>> getMem_list() {
        return mem_list;
    }

    public void setMem_list(List<Map<String, String>> mem_list) {
        this.mem_list = mem_list;
    }

    List<Map<String,String>> mem_list;
}
