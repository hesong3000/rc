package com.example.demo.config;

public class MQConstant {
    public final static String EXCHANGE = "licodeExchange";
    public final static String QUEUE_NAME = "roomController-queue";
    public final static String RC_BINDING_KEY = "RC_binding_key";
    public final static String CLIENT_KEY_PREFIX = "AV_Client_";
    public final static String MP_KEY_PREFIX = "MP_Binding_";
    public final static int USER_ONLINE_EXPIRE = 5;   //user online status expire 5s
    public final static String REDIS_ROOMS_HKEY = "AVRooms";
    public final static String REDIS_USER_KEY_PREFIX = "AV_User:";
    public final static String REDIS_MP_KEY_PREFIX = "AV_MP:";
    public final static String REDIS_ROOM_KEY_PREFIX = "AV_Room:";
}
