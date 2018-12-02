package com.example.demo.po;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MPServerInfo implements Serializable {
    public String getMp_id() {
        return mp_id;
    }

    public void setMp_id(String mp_id) {
        this.mp_id = mp_id;
    }

    public String getBinding_key() {
        return binding_key;
    }

    public void setBinding_key(String binding_key) {
        this.binding_key = binding_key;
    }

    private String mp_id = "";
    private String binding_key = "";
    private int userd_stream_count = 0;

    public int getUserd_stream_count() {
        return userd_stream_count;
    }

    public void setUserd_stream_count(int userd_stream_count) {
        this.userd_stream_count = userd_stream_count;
    }

    public int getMax_stream_count() {
        return max_stream_count;
    }

    public void setMax_stream_count(int max_stream_count) {
        this.max_stream_count = max_stream_count;
    }

    private int max_stream_count = 0;
    private int reserve_stream_count = 0;

    public int getReserve_stream_count() {
        return reserve_stream_count;
    }

    public void setReserve_stream_count(int reserve_stream_count) {
        this.reserve_stream_count = reserve_stream_count;
    }

    public Map<String, Integer> getRoom_list() {
        return room_list;
    }

    public void setRoom_list(Map<String, Integer> room_list) {
        this.room_list = room_list;
    }

    private Map<String, Integer> room_list = new HashMap<>();
}
