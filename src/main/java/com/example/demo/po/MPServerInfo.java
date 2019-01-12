package com.example.demo.po;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

    public int getMcuIdleReource(){
        int use_mcu_resource = 0;
        Iterator<Map.Entry<String, Map<String, Integer>>> room_iter = room_stream_list.entrySet().iterator();
        while(room_iter.hasNext()){
            Map<String, Integer> roomStreams = room_iter.next().getValue();
            Iterator<Map.Entry<String, Integer>> roomStream_iter = roomStreams.entrySet().iterator();
            while (roomStream_iter.hasNext()){
                int room_resource_count = roomStream_iter.next().getValue();
                use_mcu_resource = use_mcu_resource+room_resource_count;
            }
        }
        return (max_stream_count-reserve_stream_count-use_mcu_resource)<0 ? 0 : (max_stream_count-reserve_stream_count-use_mcu_resource);
    }

    private Map<String, Map<String, Integer>> room_stream_list = new HashMap<>();
    public void addMcuUseResource(String room_id, String publish_stream_id, int mcu_resource_count){
        if(room_stream_list.containsKey(room_id)==false){
            Map<String, Integer> roomStreams = new HashMap<>();
            roomStreams.put(publish_stream_id, mcu_resource_count);
            room_stream_list.put(room_id, roomStreams);
        }else{
            Map<String, Integer> roomStreams = room_stream_list.get(room_id);
            if(roomStreams.containsKey(publish_stream_id)==false){
                roomStreams.put(publish_stream_id, mcu_resource_count);
            }else
                roomStreams.put(publish_stream_id, roomStreams.get(publish_stream_id)+mcu_resource_count);
            room_stream_list.put(room_id, roomStreams);
        }
    }

    public void releaseMcuUseResource(String room_id, String publish_stream_id, Integer mcu_resource){
        if(room_stream_list.containsKey(room_id)==true){
            Map<String, Integer> roomStreams = room_stream_list.get(room_id);
            if(roomStreams.containsKey(publish_stream_id)==true){
                if(roomStreams.get(publish_stream_id)<=mcu_resource)
                    roomStreams.remove(publish_stream_id);
                else
                    roomStreams.put(publish_stream_id, roomStreams.get(publish_stream_id)-mcu_resource);
            }

            if(roomStreams.size()==0)
                room_stream_list.remove(room_id);
        }
    }

    public void clearMcuUseResourceOnRemove(String room_id, String publish_stream_id){
        if(room_stream_list.containsKey(room_id)==true){
            Map<String, Integer> roomStreams = room_stream_list.get(room_id);
            roomStreams.remove(publish_stream_id);

            if(roomStreams.size()==0)
                room_stream_list.remove(room_id);
        }
    }

    public Map<String, Map<String, Integer>> getRoom_stream_list() {
        return room_stream_list;
    }

    public void setRoom_stream_list(Map<String, Map<String, Integer>> room_stream_list) {
        this.room_stream_list = room_stream_list;
    }

    public String getSrc_domain() {
        return src_domain;
    }

    public void setSrc_domain(String src_domain) {
        this.src_domain = src_domain;
    }

    private String src_domain = "";

    public boolean isEmcu() {
        return isEmcu;
    }

    public void setEmcu(boolean emcu) {
        isEmcu = emcu;
    }

    private boolean isEmcu = false;

    public String getAval_domain() {
        return aval_domain;
    }

    public void setAval_domain(String aval_domain) {
        this.aval_domain = aval_domain;
    }

    private String aval_domain = "";

    @Override
    public Object clone() throws CloneNotSupportedException {
        MPServerInfo mpServerInfo = new MPServerInfo();
        mpServerInfo.setSrc_domain(this.src_domain);
        mpServerInfo.setAval_domain(this.aval_domain);
        mpServerInfo.setEmcu(this.isEmcu);
        mpServerInfo.setMp_id(this.mp_id);
        mpServerInfo.setBinding_key(this.binding_key);
        mpServerInfo.setMax_stream_count(this.max_stream_count);
        mpServerInfo.setReserve_stream_count(this.reserve_stream_count);
        mpServerInfo.setRoom_stream_list(new HashMap(this.room_stream_list));
        return mpServerInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MPServerInfo)) return false;

        MPServerInfo that = (MPServerInfo) o;
        //只由mcu_id唯一确定mcu
        if(this.mp_id.compareTo(that.mp_id)==0)
            return true;
        else
            return false;
    }
}
