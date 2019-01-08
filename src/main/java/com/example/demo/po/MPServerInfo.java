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

    public Map<String, Integer> getRoom_list() {
        return room_list;
    }

    public void setRoom_list(Map<String, Integer> room_list) {
        this.room_list.clear();
        this.room_list.putAll(room_list);
    }

    public void addMcuUseResource(String room_id, int mcu_resource_count){
        if(room_list.containsKey(room_id)==false){
             room_list.put(room_id,mcu_resource_count);
        }else{
            room_list.put(room_id, room_list.get(room_id)+mcu_resource_count);
        }
    }

    public int getMcuIdleReource(){
        int use_mcu_resource = 0;
        Iterator<Map.Entry<String, Integer>> iterator = room_list.entrySet().iterator();
        while (iterator.hasNext()){
            int room_resource_count = iterator.next().getValue();
            use_mcu_resource = use_mcu_resource+room_resource_count;
        }
        return (max_stream_count-reserve_stream_count-use_mcu_resource)<0 ? 0 : (max_stream_count-reserve_stream_count-use_mcu_resource);
    }

    public void releaseMcuUseResource(String room_id, Integer mcu_resource){
        if(room_list.containsKey(room_id)==true){
            if(room_list.get(room_id) <= mcu_resource)
                room_list.put(room_id, 0);
            else
                room_list.put(room_id, room_list.get(room_id)-mcu_resource);
        }
    }

    //删除会议室的时候调用
    public void clearMcuResourceOnDelete(String room_id){
        if(room_list.containsKey(room_id)==true)
            room_list.remove(room_id);
    }

    private Map<String, Integer> room_list = new HashMap<>();

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
        mpServerInfo.setRoom_list(new HashMap(this.room_list));
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
