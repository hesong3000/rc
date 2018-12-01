package com.example.demo;

import com.example.demo.config.MQConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AMQPReceiver {
    @Autowired
    RoomMsgHolder roomMsgHolder;
    private static Logger log = LoggerFactory.getLogger(AMQPReceiver.class);

    @RabbitListener(containerFactory = "rabbitListenerContainerFactory",
            bindings = @QueueBinding(value = @Queue(value = MQConstant.MQ_QUEUE_NAME, durable = "false"),
                    exchange = @Exchange(value = MQConstant.MQ_EXCHANGE,
                            type = ExchangeTypes.DIRECT), key = MQConstant.MQ_RC_BINDING_KEY))
    public void process(byte[] message){
        String msgRecv_str = new String(message);
        roomMsgHolder.pushMsg(msgRecv_str);
    }
}
