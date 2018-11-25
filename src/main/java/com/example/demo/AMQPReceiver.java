package com.example.demo;

import com.example.demo.config.MQConstant;
import com.example.demo.task.RCUserConnectTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class AMQPReceiver {
    @Autowired
    RoomMsgHolder roomMsgHolder;
    private static Logger log = LoggerFactory.getLogger(AMQPReceiver.class);

    @RabbitListener(containerFactory = "rabbitListenerContainerFactory",
            bindings = @QueueBinding(value = @Queue(value = MQConstant.QUEUE_NAME, durable = "true"),
                    exchange = @Exchange(value = MQConstant.EXCHANGE,
                            type = ExchangeTypes.DIRECT), key = MQConstant.RC_BINDING_KEY))
    public void process(String msgRecv){
        log.info("amqp recv msg {}",msgRecv);
        roomMsgHolder.pushMsg(msgRecv);
    }
}
