package com.example.demo.po;

import java.io.Serializable;

public class AVStreamInfo implements Serializable{
    private String stream_id = "";
    private String room_id = "";
    private Boolean screencast = false;
    private Boolean audioMuted = false;
    private Boolean videoMuted = false;
    private String mcu_bingkey = "";

    public String getPublisher_id() {
        return publisher_id;
    }

    public void setPublisher_id(String publisher_id) {
        this.publisher_id = publisher_id;
    }

    private String publisher_id = "";

    public Boolean getPublish() {
        return isPublish;
    }

    public void setPublish(Boolean publish) {
        isPublish = publish;
    }

    private Boolean isPublish = true;

    public String getMcu_bingkey() {
        return mcu_bingkey;
    }

    public void setMcu_bingkey(String mcu_bingkey) {
        this.mcu_bingkey = mcu_bingkey;
    }

    public String getStream_id() {
        return stream_id;
    }

    public void setStream_id(String stream_id) {
        this.stream_id = stream_id;
    }

    public String getRoom_id() {
        return room_id;
    }

    public void setRoom_id(String room_id) {
        this.room_id = room_id;
    }

    public Boolean getScreencast() {
        return screencast;
    }

    public void setScreencast(Boolean screencast) {
        this.screencast = screencast;
    }

    public Boolean getAudioMuted() {
        return audioMuted;
    }

    public void setAudioMuted(Boolean audioMuted) {
        this.audioMuted = audioMuted;
    }

    public Boolean getVideoMuted() {
        return videoMuted;
    }

    public void setVideoMuted(Boolean videoMuted) {
        this.videoMuted = videoMuted;
    }
}
