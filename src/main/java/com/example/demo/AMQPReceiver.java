package com.example.demo;

import com.example.demo.config.MQConstant;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RabbitListener(containerFactory = "rabbitListenerContainerFactory",
        bindings = @QueueBinding(value = @Queue(value = MQConstant.QUEUE_NAME, durable = "true"),
                exchange = @Exchange(value = MQConstant.EXCHANGE,
                        type = ExchangeTypes.DIRECT), key = MQConstant.RC_BINDING_KEY))
public class AMQPReceiver {
    @Autowired
    RoomMsgHolder roomMsgHolder;

    @RabbitHandler
    public void process(String msgRecv){
        roomMsgHolder.pushMsg(msgRecv);
    }
}
