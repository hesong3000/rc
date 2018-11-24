package com.example.demo.config;

public class MQMessageType {
    //connection about
    public final static String type_amqp_connect_request = "amqp_connect_request";
    public final static String type_amqp_connect_response = "amqp_connect_response";
    public final static String type_amqp_disconn_request = "amqp_disconn_request";
    public final static String type_mp_connect_request = "mp_connect_request";

    //room about
    public final static String type_create_room = "create_room";
    public final static String type_room_create_response = "room_create_reponse";
    public final static String type_room_invite_notice = "room_invite_notice";
    public final static String type_accept_room = "room_accept";
    public final static String type_room_accept_response = "room_accept_response";
    public final static String type_reject_room = "room_reject";
    public final static String type_room_memberin_notice = "room_memberin_notice";
    public final static String type_room_memberout_notice = "room_memberout_notice";
    public final static String type_exit_room = "exit_room";
    public final static String type_end_room = "end_room";
    public final static String type_room_end_notice = "room_end_notice";

    //stream about
    public final static String type_mute_stream = "mute_stream";
    public final static String type_stream_muted_notice = "stream_muted_notice";
    public final static String type_add_publisher = "add_publisher";
    public final static String type_publish_initialize = "publish_initialize";
    public final static String type_publish_ready = "publish_ready";
    public final static String type_add_subscriber = "add_subscriber";
    public final static String type_subscriber_initialize = "subscriber_initialize";
    public final static String type_subscribe_ready = "subscribe_ready";
    public final static String type_stream_add_notice = "stream_add_notice";
    public final static String type_remove_publisher = "remove_publisher";
    public final static String type_remove_subscriber = "remove_subscriber";
    public final static String type_stream_removed_notice = "stream_removed_notice";
    public final static String type_get_all_publish_streams = "get_all_publish_streams";
    public final static String type_get_all_publish_streams_response =
            "get_all_publish_streams_response";
}
