package com.example.demo.po;

import com.example.demo.task.RCUserConnectTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PublishStreamInfo implements Serializable {
    private String publish_streamid = "";
    private String publish_clientid = "";
    private boolean screencast = false;

    private Map<String, String> subscribers = new HashMap<>();
    private boolean audioMuted = false;
    private boolean videoMuted = false;

    public String getStream_process_mcuid() {
        return stream_process_mcuid;
    }

    public void setStream_process_mcuid(String stream_process_mcuid) {
        this.stream_process_mcuid = stream_process_mcuid;
    }

    private String stream_process_mcuid = "";

    public String getPublish_streamid() {
        return publish_streamid;
    }

    public void setPublish_streamid(String publish_streamid) {
        this.publish_streamid = publish_streamid;
    }

    public String getPublish_clientid() {
        return publish_clientid;
    }

    public void setPublish_clientid(String publish_clientid) {
        this.publish_clientid = publish_clientid;
    }

    public boolean isScreencast() {
        return screencast;
    }

    public void setScreencast(boolean screencast) {
        this.screencast = screencast;
    }

    public Map<String, String> getSubscribers() {
        return subscribers;
    }

    public void setSubscribers(Map<String, String> subscribers) {
        this.subscribers = subscribers;
    }

    public boolean isAudioMuted() {
        return audioMuted;
    }

    public void setAudioMuted(boolean audioMuted) {
        this.audioMuted = audioMuted;
    }

    public boolean isVideoMuted() {
        return videoMuted;
    }

    public void setVideoMuted(boolean videoMuted) {
        this.videoMuted = videoMuted;
    }
}
