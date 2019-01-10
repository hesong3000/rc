package com.example.demo.task;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.demo.config.AVErrorType;
import com.example.demo.config.DomainDefineBean;
import com.example.demo.config.MQConstant;
import com.example.demo.po.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.*;

@Component(value= MCUAddSubscriberTask.taskType)
@Scope("prototype")
public class MCUAddSubscriberTask extends SimpleTask implements Runnable {
    private static Logger log = LoggerFactory.getLogger(MCUAddSubscriberTask.class);
    public final static String taskType = "add_subscriber";
    public final static String taskFailType = "msg_fail_response";
    public final static String cascadeSubscribeType = "mcu_cascade_subscribe_request";
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    @Autowired
    private AmqpTemplate rabbitTemplate;
    @Autowired
    DomainDefineBean domainBean;
    /*
    1、相关会议室校验，回复失败信息
    2、选择一个MCU（发布流所在的mcu，判定mcu使用率是否满足需求）发送add_subscriber请求，消息体中存在冗余信息，可以不用改变
    */

    /*
        跨域级联增加业务逻辑（会议室所在域为“本域”）,最多能实现两级域的订阅
        add_subscriber（mcu选择、mcu跨域级联）均由会议室统一处理，首先将请求发送至会议室所在域，需要考虑以下情况：
        1、conditionOne: 待订阅的发布流和会议室都在本域，订阅终端也在本域，则直接在持有发布流的mcu上处理订阅请求即可；
        2、conditionTwo: 待订阅的发布流不在本域（在外域的emcu上），订阅终端和会议室在本域,则先(a)选择本域的合适的mcu
                         处理级联订阅（addCascadeSubscribe）请求; (b)选择对应外域的emcu处理级联订阅请求;(c)本域emcu向外域emcu做订阅处理
                         a、b、c三阶段并行执行，按媒体流的源站情况可减少执行阶段。目前只能对本域的mcu和emcu的资源进行逻辑判断，最终外域的发布流
                         会发布到本域的mcu上才算完成（为了不占用对应外域的emcu资源）。
        3、conditionThree: 待订阅的发布流和会议室在本域，订阅终端不在本域
        4、conditionFour: 待订阅的发布流和订阅终端在同一个外域，会议室在本域，直接在外域的emcu作订阅处理
        5、conditionFive： 待订阅的发布流、订阅终端、会议室分别在不同的域，暂不处理，直接回复mcu资源不足
     */
    private int processRequest(JSONObject requestMsg, Result result){
        String room_id = requestMsg.getString("room_id");
        String client_id = requestMsg.getString("client_id");
        String stream_id = requestMsg.getString("stream_id");
        String publish_stream_id = requestMsg.getString("publish_stream_id");
        JSONObject jsonOption = requestMsg.getJSONObject("options");
        if(room_id==null||client_id==null||stream_id==null||publish_stream_id==null
                ||jsonOption==null){
            log.error("add_subscriber msg lack params, msg: {}",requestMsg);
            return AVErrorType.ERR_PARAM_REQUEST;
        }

        String client_domain = "";
        if(requestMsg.containsKey("src_domain")==true)
            client_domain = requestMsg.getString("src_domain");
        else
            client_domain = domainBean.getSrcDomain();

        result.request_client_domain = client_domain;
        result.client_id = client_id;
        result.publish_stream_id = publish_stream_id;
        String client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
        requestMsg.put("client_bindkey", client_bindkey);
        //检查AVStreamInfo是否存在，不存在为代码BUG
        String avstream_key = MQConstant.REDIS_STREAM_KEY_PREFIX+publish_stream_id;
        AVStreamInfo avStreamInfo = (AVStreamInfo)RedisUtils.get(redisTemplate, avstream_key);
        if(avStreamInfo==null){
            log.error("redis get avstream failed, key: {}", avstream_key);
            return AVErrorType.ERR_STREAM_SHUTDOWN;
        }

        String room_domain = avStreamInfo.getRoom_domain();
        String publish_client_id = avStreamInfo.getPublisher_id();

        //若room_domain不在本域，则发送跨域请求
        if(room_domain.compareTo(domainBean.getSrcDomain())!=0){
            //选择合适的emcu处理订阅请求
            MPServerInfo avail_emcu = null;
            int has_idle_stream_count = 0;
            String avMPs_key = MQConstant.REDIS_MPINFO_HASH_KEY;
            Map<Object,Object> av_mps = RedisUtils.hmget(redisTemplate,avMPs_key);
            Iterator<Map.Entry<Object, Object>> mcu_iter = av_mps.entrySet().iterator();
            while (mcu_iter.hasNext()){
                MPServerInfo mpServerInfo = (MPServerInfo)mcu_iter.next().getValue();
                int mp_idle_stream_count = mpServerInfo.getMcuIdleReource();
                boolean isEmcu = mpServerInfo.isEmcu();
                String emcu_avaliable_domain = mpServerInfo.getAval_domain();
                if(isEmcu==false || emcu_avaliable_domain.compareTo(room_domain)!=0)
                    continue;
                if(mp_idle_stream_count>has_idle_stream_count){
                    has_idle_stream_count = mp_idle_stream_count;
                    avail_emcu = mpServerInfo;
                }
            }
            if(avail_emcu==null || avail_emcu.getMcuIdleReource()<=0)
                return AVErrorType.ERR_MCURES_NOT_ENOUGH;

            requestMsg.put("sub_emcu_id",avail_emcu.getMp_id());
            requestMsg.put("sub_emcu_domain",avail_emcu.getSrc_domain());
            DomainRoute domainRoute = domainBean.getDstDomainRoute(room_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        MCUAddSubscriberTask.taskType, room_domain, requestMsg);
                return AVErrorType.ERR_ROUTE_NOTEXIST;
            }
            requestMsg.put("src_domain", domainBean.getSrcDomain());
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            requestMsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, requestMsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", requestMsg);
            return AVErrorType.ERR_NOERROR;
        }

        //请求发送至会议室所在域，进行后续处理逻辑
        //检查会议室是否存在
        String avRoomsKey = MQConstant.REDIS_AVROOMS_KEY;
        String avRoomItem = MQConstant.REDIS_ROOM_KEY_PREFIX+room_id;
        AVLogicRoom avLogicRoom = (AVLogicRoom)RedisUtils.hget(redisTemplate, avRoomsKey, avRoomItem);
        if(avLogicRoom == null){
            return AVErrorType.ERR_ROOM_NOTEXIST;
        }

        //检查会议室是否有此成员
        Map<String, RoomMemInfo> roomMemInfoMap = avLogicRoom.getRoom_mems();
        if(roomMemInfoMap.containsKey(client_id) == false)
            return AVErrorType.ERR_ROOM_KICK;

        if(roomMemInfoMap.containsKey(publish_client_id)==false)
            return AVErrorType.ERR_ROOM_KICK;

        //检查会议室中是否发布了此媒体流
        if(avLogicRoom.getPublish_streams().containsKey(publish_stream_id)==false){
            return AVErrorType.ERR_STREAM_SHUTDOWN;
        }

        String sub_emcu_id = "";
        String sub_emcu_domain = "";
        if(requestMsg.containsKey("sub_emcu_id"))
            sub_emcu_id = requestMsg.getString("sub_emcu_id");
        if(requestMsg.containsKey("sub_emcu_domain"))
            sub_emcu_domain = requestMsg.getString("sub_emcu_domain");
        result.sub_emcu_id = sub_emcu_id;
        result.sub_emcu_domain = sub_emcu_domain;
        //判断订阅流属于哪一种情况
        PublishStreamInfo publishStreamInfo = avLogicRoom.getPublish_streams().get(publish_stream_id);
        Map<String, List<MPServerInfo>> publishStream_resources = publishStreamInfo.getPublish_stream_resources();
        //订阅终端所在域
        String subscriber_domain = client_domain;
        String publisher_domain = roomMemInfoMap.get(publish_client_id).getMem_domain();
        int publisher_domain_size = publishStream_resources.size();
        if(publisher_domain_size == 0){
            //若是如此则出现BUG，出现业务数据不一致
            log.error("can not find publishstream mcu resource");
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;
        }

        //发布流成功后的源站只可能在本域的mcu上或外域的emcu上
        //符合媒体流已发布至本域mcu的列表(资源可计算)
        List<MPServerInfo> roomDomain_mcus = new LinkedList<>();
        //符合媒体流在发布源域emcu的列表(资源不可计算)
        List<MPServerInfo> publisherDomain_emcus = new LinkedList<>();
        Iterator<Map.Entry<String, List<MPServerInfo>>> resource_iter = publishStream_resources.entrySet().iterator();
        while(resource_iter.hasNext()){
            Map.Entry<String, List<MPServerInfo>> entry = resource_iter.next();
            List<MPServerInfo> mpServerInfos = entry.getValue();
            if(mpServerInfos.size()==0){
                //若是如此则出现BUG，出现业务数据不一致
                log.error("can not find publishstream mcu resource");
                return AVErrorType.ERR_MCURES_NOT_ENOUGH;
            }
            int mpServerInfos_size = mpServerInfos.size();
            for(int mcu_index=0;mcu_index<mpServerInfos_size;mcu_index++){
                MPServerInfo mpServerInfo = mpServerInfos.get(mcu_index);
                String mcu_domain = entry.getKey();
                //表示符合媒体流发布到room域mcu的源站列表，满足情况1和情况3的媒体源站需求
                if(room_domain.compareTo(mcu_domain)==0 && mpServerInfo.isEmcu()==false)
                    roomDomain_mcus.add(mpServerInfo);

                //表示外域媒体流发布到外域emcu或room域媒体流发布到外域emcu的源站列表，可满足情况4、情况2和情况5的媒体源站需求
                if((publisher_domain.compareTo(mcu_domain)==0 || subscriber_domain.compareTo(mcu_domain)==0)
                        && mpServerInfo.isEmcu()==true)
                    publisherDomain_emcus.add(mpServerInfo);
            }
        }

        int mcu_idle_resource = 0;
        MPServerInfo avaliable_mcu = null;
        if(subscriber_domain.compareTo(room_domain)==0){
            //订阅终端在本域，首先从本域订阅流，不满足时从发布流源域级联方式订阅流。优先1、次2
            //计算1资源
            int roomDomainmcu_size = roomDomain_mcus.size();
            for(int mcu_index=0;mcu_index<roomDomainmcu_size;mcu_index++){
                MPServerInfo mpServerInfo = roomDomain_mcus.get(mcu_index);
                if(mpServerInfo.getMcuIdleReource()>mcu_idle_resource){
                    mcu_idle_resource = mpServerInfo.getMcuIdleReource();
                    avaliable_mcu = mpServerInfo;
                }
            }
            //获取满足条件且资源占用最少的mcu处理订阅请求，直接订阅
            if(avaliable_mcu!=null && avaliable_mcu.getMcuIdleReource()>0)
                return procConditionOne(avaliable_mcu, avLogicRoom, requestMsg);

            //从发布流源域级联方式订阅流，级联订阅
            int publisherDomain_emcus_size = publisherDomain_emcus.size();
            for(int mcu_index=0;mcu_index<publisherDomain_emcus_size;mcu_index++){
                MPServerInfo mpServerInfo = publisherDomain_emcus.get(mcu_index);
                //2无法计算资源，存在即合理
                if(mpServerInfo.getSrc_domain().compareTo(publisher_domain)==0) {
                    avaliable_mcu = mpServerInfo;
                    return procConditionTwo(avaliable_mcu, avLogicRoom, requestMsg);
                }
            }

            return AVErrorType.ERR_MCURES_NOT_ENOUGH;
        }else{
            //优先4、次3、再次5
            //从订阅终端所在域的emcu获取媒体流源站，直接订阅
            int publisherDomainEmcu_size = publisherDomain_emcus.size();
            for(int mcu_index=0;mcu_index<publisherDomainEmcu_size;mcu_index++){
                //资源无法计算，存在即合理
                if(publisherDomain_emcus.get(mcu_index).getSrc_domain().compareTo(subscriber_domain)==0){
                    return procConditionFour(publisherDomain_emcus.get(mcu_index), avLogicRoom, requestMsg);
                }
            }

            //从room域的mcu（占用资源最少）获取媒体流源站，级联订阅
            avaliable_mcu = null;
            mcu_idle_resource = 0;
            int roomDomainmcu_size = roomDomain_mcus.size();
            for(int mcu_index=0;mcu_index<roomDomainmcu_size;mcu_index++){
                MPServerInfo mpServerInfo = roomDomain_mcus.get(mcu_index);
                if(mpServerInfo.getMcuIdleReource()>mcu_idle_resource){
                    mcu_idle_resource = mpServerInfo.getMcuIdleReource();
                    avaliable_mcu = mpServerInfo;
                }
            }
            if(avaliable_mcu!=null && avaliable_mcu.getMcuIdleReource()>0)
                return procConditionThree(avaliable_mcu, avLogicRoom, requestMsg, result);

            //从发布流源域级联方式订阅流，两次级联订阅：1是从发布流所在域的emcu级联订阅到本域mcu，2是从本域mcu级联订阅到订阅端所在域的emcu
            int publisherDomain_emcus_size = publisherDomain_emcus.size();
            for(int mcu_index=0;mcu_index<publisherDomain_emcus_size;mcu_index++){
                MPServerInfo mpServerInfo = publisherDomain_emcus.get(mcu_index);
                //2无法计算资源，存在即合理
                if(mpServerInfo.getSrc_domain().compareTo(publisher_domain)==0) {
                    avaliable_mcu = mpServerInfo;
                    return procConditionFive(avaliable_mcu, avLogicRoom, requestMsg, result);
                }
            }

            return AVErrorType.ERR_MCURES_NOT_ENOUGH;
        }
    }

    //第一种情况: 会议室和订阅终端在本域，且发布流在本域mcu或已经发布到本域的mcu，一次订阅即可
    private int procConditionOne(MPServerInfo mpServerInfo, AVLogicRoom avLogicRoom, JSONObject orign_msg){
        //直接向MCU发送addSubscriber请求
        JSONObject request_msg = (JSONObject)orign_msg.clone();
        String client_id = request_msg.getString("client_id");
        String mcu_bindkey = mpServerInfo.getBinding_key();
        String client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
        request_msg.put("client_bindkey", client_bindkey);
        log.info("mq send to mcu {}: {}", mcu_bindkey,request_msg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, request_msg);
        return AVErrorType.ERR_NOERROR;
    }

    //第四种情况: 会议室在本域，订阅终端和发布流在同一个外域，且发布流在外域的emcu上，一次订阅即可
    private int procConditionFour(MPServerInfo mpServerInfo, AVLogicRoom avLogicRoom, JSONObject orign_msg){
        JSONObject request_msg = (JSONObject)orign_msg.clone();
        String client_id = request_msg.getString("client_id");
        String client_bindkey = MQConstant.MQ_CLIENT_KEY_PREFIX+client_id;
        request_msg.put("client_bindkey", client_bindkey);
        String dst_domain = mpServerInfo.getSrc_domain();
        String mcu_id = mpServerInfo.getMp_id();
        DomainRoute domainRoute = domainBean.getDstDomainRoute(dst_domain);
        if(domainRoute==null){
            log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                    MCUAddSubscriberTask.taskType, dst_domain, request_msg);
            return AVErrorType.ERR_ROUTE_NOTEXIST;
        }
        JSONObject crossDomainmsg = new JSONObject();
        crossDomainmsg.put("type", CDCrossDomainToMCUTask.taskType);
        crossDomainmsg.put("mcu_id", mcu_id);
        crossDomainmsg.put("encap_msg", request_msg);
        List<DomainRoute> new_domain_list = new LinkedList<>();
        new_domain_list.add(domainRoute);
        JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
        crossDomainmsg.put("domain_route", domain_array);
        log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
        rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
        return AVErrorType.ERR_NOERROR;
    }

    //第二种情况： 发布流不在本域（在外域的emcu上），订阅终端和会议室在本域，需将发布流发布到本域的mcu上，
    //           过程：一次级联订阅、一次订阅
    private int procConditionTwo(MPServerInfo srcServerInfo, AVLogicRoom avLogicRoom, JSONObject orign_msg){
        String publish_stream_id = orign_msg.getString("publish_stream_id");
        //选择本域合适的mcu和对应发布流域的emcu
        int has_idle_stream_count = 0;
        int has_idle_stream_count2 = 0;
        MPServerInfo avail_mcu = null;
        MPServerInfo avail_emcu2 = null;
        String avMPs_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        Map<Object,Object> av_mps = RedisUtils.hmget(redisTemplate,avMPs_key);
        Iterator<Map.Entry<Object, Object>> mcu_iter = av_mps.entrySet().iterator();
        while (mcu_iter.hasNext()){
            MPServerInfo mpServerInfo = (MPServerInfo)mcu_iter.next().getValue();
            boolean isEmcu = mpServerInfo.isEmcu();
            String emcu_avaliable_domain = mpServerInfo.getAval_domain();
            int mp_idle_stream_count = mpServerInfo.getMcuIdleReource();
            if(isEmcu==false){
                if(mp_idle_stream_count>has_idle_stream_count){
                    has_idle_stream_count = mp_idle_stream_count;
                    avail_mcu = mpServerInfo;
                }
            }else{
                if(emcu_avaliable_domain.compareTo(srcServerInfo.getSrc_domain())==0){
                    if(mp_idle_stream_count>has_idle_stream_count2){
                        has_idle_stream_count2 = mp_idle_stream_count;
                        avail_emcu2 = mpServerInfo;
                    }
                }
            }
        }

        if(avail_mcu==null || avail_emcu2==null || avail_mcu.getMcuIdleReource()<=0 || avail_emcu2.getMcuIdleReource()<=0)
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;

        //cascade_mcus： avail_mcu、avail_emcu2、srcServerInfo 有序
        JSONObject cascadeSubscribe_msg = new JSONObject();
        cascadeSubscribe_msg.put("type", MCUAddSubscriberTask.cascadeSubscribeType);
        cascadeSubscribe_msg.put("cascade_route", 0);
        cascadeSubscribe_msg.put("stream_id", publish_stream_id);
        cascadeSubscribe_msg.put("ecap_msg", orign_msg);
        JSONArray mcu_array = new JSONArray();
        JSONObject mcu_0 = new JSONObject();
        mcu_0.put("mcu_id", avail_mcu.getMp_id());
        mcu_0.put("mcu_domain", avail_mcu.getSrc_domain());
        mcu_0.put("route_index", 0);
        mcu_array.add(mcu_0);
//        JSONObject mcu_1 = new JSONObject();
//        mcu_1.put("mcu_id", avail_emcu2.getMp_id());
//        mcu_1.put("mcu_domain", avail_emcu2.getSrc_domain());
//        mcu_1.put("route_index", 1);
//        mcu_array.add(mcu_1);
        JSONObject mcu_2 = new JSONObject();
        mcu_2.put("mcu_id", srcServerInfo.getMp_id());
        mcu_2.put("mcu_domain", srcServerInfo.getSrc_domain());
        mcu_2.put("route_index", 1);
        mcu_array.add(mcu_2);
        cascadeSubscribe_msg.put("cascade_mcus", mcu_array);

        //将级联订阅消息发送至第一个从订阅终端方向开始的mcu
        String mcu_domain = avail_mcu.getSrc_domain();
        if(mcu_domain.compareTo(domainBean.getSrcDomain())==0){
            String mcu_id = avail_mcu.getMp_id();
            String mcu_bindkey = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
            log.info("mq send to mcu {}: {}", mcu_bindkey,cascadeSubscribe_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, cascadeSubscribe_msg);
        }else{
            //跨域发送至mcu
            DomainRoute domainRoute = domainBean.getDstDomainRoute(mcu_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        MCUMuteStreamTask.taskType, mcu_domain, cascadeSubscribe_msg);
                return AVErrorType.ERR_ROUTE_NOTEXIST;
            }

            JSONObject crossDomainmsg = new JSONObject();
            crossDomainmsg.put("type", CDCrossDomainToMCUTask.taskType);
            crossDomainmsg.put("mcu_id", avail_mcu.getMp_id());
            crossDomainmsg.put("encap_msg", cascadeSubscribe_msg);
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            crossDomainmsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
        }

        //更新逻辑会议室的publishstreaminfo信息 avail_mcu、avail_emcu2
        avLogicRoom.getPublish_streams().get(publish_stream_id).addPublish_stream_resource(avail_mcu);
        avLogicRoom.getPublish_streams().get(publish_stream_id).addPublish_stream_resource(avail_emcu2);
        //将更新后的逻辑会议室信息存储
        String avRooms_key = MQConstant.REDIS_AVROOMS_KEY;
        String avRoom_hashKey = MQConstant.REDIS_ROOM_KEY_PREFIX+avLogicRoom.getRoom_id();
        if(RedisUtils.hset(redisTemplate, avRooms_key, avRoom_hashKey, avLogicRoom)==false){
            log.error("redis hset avroominfo failed, key: {} hashket: {}, value: {}",
                    avRooms_key, avRoom_hashKey, avLogicRoom);
            return AVErrorType.ERR_REDIS_STORE;
        }

        return AVErrorType.ERR_NOERROR;
    }

    //第三种情况： 发布流（在本域的mcu上）和会议室在本域，订阅终端在外域，需将发布流发布到订阅终端域的emcu上，
    //           过程：一次级联订阅、一次订阅
    private int procConditionThree(MPServerInfo srcServerInfo, AVLogicRoom avLogicRoom, JSONObject orign_msg, Result result){
        if(result.sub_emcu_id.length()==0 || result.sub_emcu_domain.length()==0)
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;
        String publish_stream_id = orign_msg.getString("publish_stream_id");

        //选择本域合适的emcu
        int has_idle_stream_count = 0;
        MPServerInfo avail_emcu = null;
        String avMPs_key = MQConstant.REDIS_MPINFO_HASH_KEY;
        Map<Object,Object> av_mps = RedisUtils.hmget(redisTemplate,avMPs_key);
        Iterator<Map.Entry<Object, Object>> mcu_iter = av_mps.entrySet().iterator();
        while (mcu_iter.hasNext()){
            MPServerInfo mpServerInfo = (MPServerInfo)mcu_iter.next().getValue();
            boolean isEmcu = mpServerInfo.isEmcu();
            String emcu_avaliable_domain = mpServerInfo.getAval_domain();
            int mp_idle_stream_count = mpServerInfo.getMcuIdleReource();
            if(isEmcu==true && emcu_avaliable_domain.compareTo(result.sub_emcu_domain)==0){
                if(mp_idle_stream_count>has_idle_stream_count){
                    has_idle_stream_count = mp_idle_stream_count;
                    avail_emcu = mpServerInfo;
                }
            }
        }

        if(avail_emcu==null)
            return AVErrorType.ERR_MCURES_NOT_ENOUGH;

        //将级联订阅消息发送至第一个从订阅终端方向开始的mcu
        //cascade_mcus： result_sub_mcu、avail_emcu、srcServerInfo 有序
        JSONObject cascadeSubscribe_msg = new JSONObject();
        cascadeSubscribe_msg.put("type", MCUAddSubscriberTask.cascadeSubscribeType);
        cascadeSubscribe_msg.put("cascade_route", 0);
        cascadeSubscribe_msg.put("stream_id", publish_stream_id);
        cascadeSubscribe_msg.put("ecap_msg", orign_msg);
        JSONArray mcu_array = new JSONArray();
        JSONObject mcu_0 = new JSONObject();
        mcu_0.put("mcu_id", result.sub_emcu_id);
        mcu_0.put("mcu_domain", result.sub_emcu_domain);
        mcu_0.put("route_index", 0);
        mcu_array.add(mcu_0);
        JSONObject mcu_1 = new JSONObject();
        mcu_1.put("mcu_id", avail_emcu.getMp_id());
        mcu_1.put("mcu_domain", avail_emcu.getSrc_domain());
        mcu_1.put("route_index", 1);
        mcu_array.add(mcu_1);
        JSONObject mcu_2 = new JSONObject();
        mcu_2.put("mcu_id", srcServerInfo.getMp_id());
        mcu_2.put("mcu_domain", srcServerInfo.getSrc_domain());
        mcu_2.put("route_index", 2);
        mcu_array.add(mcu_2);
        cascadeSubscribe_msg.put("cascade_mcus", mcu_array);

        //将级联订阅消息发送至第一个从订阅终端方向开始的mcu
        String mcu_domain = result.sub_emcu_domain;
        String mcu_id = result.sub_emcu_id;
        if(mcu_domain.compareTo(domainBean.getSrcDomain())==0){
            String mcu_bindkey = MQConstant.MQ_MCU_KEY_PREFIX+mcu_id;
            log.info("mq send to mcu {}: {}", mcu_bindkey,cascadeSubscribe_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, mcu_bindkey, cascadeSubscribe_msg);
        }else{
            //跨域发送至mcu
            DomainRoute domainRoute = domainBean.getDstDomainRoute(mcu_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        MCUMuteStreamTask.taskType, mcu_domain, cascadeSubscribe_msg);
                return AVErrorType.ERR_ROUTE_NOTEXIST;
            }

            JSONObject crossDomainmsg = new JSONObject();
            crossDomainmsg.put("type", CDCrossDomainToMCUTask.taskType);
            crossDomainmsg.put("mcu_id", mcu_id);
            crossDomainmsg.put("encap_msg", cascadeSubscribe_msg);
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            crossDomainmsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
        }

        MPServerInfo sub_mcu = new MPServerInfo();
        sub_mcu.setMp_id(result.sub_emcu_id);
        sub_mcu.setSrc_domain(result.sub_emcu_domain);
        sub_mcu.setEmcu(true);
        //更新逻辑会议室的publishstreaminfo信息 result_sub_mcu、avail_emcu
        avLogicRoom.getPublish_streams().get(publish_stream_id).addPublish_stream_resource(sub_mcu);
        avLogicRoom.getPublish_streams().get(publish_stream_id).addPublish_stream_resource(avail_emcu);
        //将更新后的逻辑会议室信息存储
        String avRooms_key = MQConstant.REDIS_AVROOMS_KEY;
        String avRoom_hashKey = MQConstant.REDIS_ROOM_KEY_PREFIX+avLogicRoom.getRoom_id();
        if(RedisUtils.hset(redisTemplate, avRooms_key, avRoom_hashKey, avLogicRoom)==false){
            log.error("redis hset avroominfo failed, key: {} hashket: {}, value: {}",
                    avRooms_key, avRoom_hashKey, avLogicRoom);
            return AVErrorType.ERR_REDIS_STORE;
        }
        return AVErrorType.ERR_NOERROR;
    }

    //第五种情况： 发布流、会议室、订阅终端分别在三个域，两次级联订阅、一次订阅，第二、三种情况的融合
    private int procConditionFive(MPServerInfo mpServerInfo, AVLogicRoom avLogicRoom, JSONObject orign_msg, Result result){
        //暂不实现
        return AVErrorType.ERR_MCURES_NOT_ENOUGH;
    }

    private int sendFailResponse(int processCode, Result result){
        if(processCode==AVErrorType.ERR_NOERROR)
            return processCode;

        if(result.client_id.length()==0)
            return processCode;

        JSONObject response_msg = new JSONObject();
        response_msg.put("type", MCUAddSubscriberTask.taskFailType);
        response_msg.put("client_id", result.client_id);
        response_msg.put("stream_id", result.publish_stream_id);
        response_msg.put("retcode", processCode);
        response_msg.put("is_publisher", false);

        if(result.request_client_domain.compareTo(domainBean.getSrcDomain())==0){
            String client_bindingkey = MQConstant.MQ_CLIENT_KEY_PREFIX+result.client_id;
            log.info("mq send response {}: {}", client_bindingkey,response_msg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_EXCHANGE, client_bindingkey, response_msg);
        }else{
            DomainRoute domainRoute = domainBean.getDstDomainRoute(result.request_client_domain);
            if(domainRoute==null){
                log.warn("{} send msg failed while can not find available domain route to {}, msg: {}",
                        RCEnterRoomTask.taskType, result.request_client_domain, response_msg);
                return processCode;
            }

            JSONObject crossDomainmsg = new JSONObject();
            crossDomainmsg.put("type", CDCrossDomainMsgTask.taskType);
            crossDomainmsg.put("client_id", result.client_id);
            crossDomainmsg.put("encap_msg", response_msg);
            List<DomainRoute> new_domain_list = new LinkedList<>();
            new_domain_list.add(domainRoute);
            JSONArray domain_array = JSONArray.parseArray(JSONObject.toJSONString(new_domain_list));
            crossDomainmsg.put("domain_route", domain_array);
            log.info("send msg to {}, msg {}", MQConstant.MQ_DOMAIN_EXCHANGE, crossDomainmsg);
            rabbitTemplate.convertAndSend(MQConstant.MQ_DOMAIN_EXCHANGE, "", crossDomainmsg);
        }
        return processCode;
    }

    @Override
    public void run() {
        log.info("execute MCUAddSubscriberTask at {}", new Date());
        JSONObject requestMsg = JSON.parseObject(msg);
        int processCode = AVErrorType.ERR_NOERROR;
        Result result = new Result();
        processCode = processRequest(requestMsg,result);
        sendFailResponse(processCode,result);
    }

    class Result{
        String client_id = "";
        String publish_stream_id = "";
        String request_client_domain = "";
        String sub_emcu_id = "";
        String sub_emcu_domain = "";
    }
}
