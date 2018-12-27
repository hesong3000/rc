package com.example.demo.config;

public class AVErrorType {
    public final static int ERR_NOERROR = 0;
    public final static int ERR_ROOMID_CONFLICT = 101;      //会议室已存在
    public final static int ERR_PARAM_REQUEST = 102;        //信令参数错误
    public final static int ERR_REDIS_STORE = 103;           //内存数据库服务异常
    public final static int ERR_ROOM_NOTEXIST = 104;        //会议室不存在
    public final static int ERR_ROOM_KICK = 105;            //用户已被剔除
    public final static int ERR_STREAM_CONFLICT = 106;      //发布的媒体流冲突
    public final static int ERR_MCURES_NOT_ENOUGH = 107;    //MCU资源不足
    public final static int ERR_STREAM_PUBLISH = 108;       //发布流过程出错
    public final static int ERR_STREAM_SUBSCRIBE = 109;     //订阅流过程出错
    public final static int ERR_STREAM_SHUTDOWN = 110;      //发布流已被关闭
    public final static int ERR_ROOM_BUSY = 111;            //会议室仍在使用中,不能删除
}
