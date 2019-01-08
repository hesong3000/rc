package com.example.demo.po;

import com.example.demo.task.MCUAddPublishTask;
import com.example.demo.task.RCUserConnectTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PublishStreamInfo implements Serializable {
    private static Logger log = LoggerFactory.getLogger(PublishStreamInfo.class);
    private String publish_streamid = "";
    private String publish_clientid = "";
    private boolean screencast = false;
    private Map<String, String> subscribers = new HashMap<>();
    private boolean audioMuted = false;
    private boolean videoMuted = false;
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
        //this.subscribers = subscribers;
        this.subscribers.clear();
        this.subscribers.putAll(subscribers);
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


    //下面两项为待删除项，由第三项代替
    private String src_domain;
    public String getSrc_domain() {
        return src_domain;
    }
    public void setSrc_domain(String src_domain) {
        this.src_domain = src_domain;
    }


    //此项表示媒体流的源站信息，key为domain_id，value表示媒体流在该域存在于哪个mcu上
    private Map<String, List<MPServerInfo>> publish_stream_resources = new HashMap<>();
    public Map<String, List<MPServerInfo>> getPublish_stream_resources() {
        return publish_stream_resources;
    }

    //增加针对此发布流的源站信息
    public void addPublish_stream_resource(MPServerInfo mpServerInfo){
        if(publish_stream_resources.containsKey(mpServerInfo.getSrc_domain())==false){
            List<MPServerInfo> mpServerInfos = new LinkedList<>();
            mpServerInfos.add(mpServerInfo);
            publish_stream_resources.put(mpServerInfo.getSrc_domain(), mpServerInfos);
        }else{
            List<MPServerInfo> mpServerInfos = publish_stream_resources.get(mpServerInfo.getSrc_domain());
            if(mpServerInfos.contains(mpServerInfo)==false){
                mpServerInfos.add(mpServerInfo);
            }else{
                //此处出现了逻辑问题
                log.warn("publish stream mcu duplicate");
            }
        }
    }
}
