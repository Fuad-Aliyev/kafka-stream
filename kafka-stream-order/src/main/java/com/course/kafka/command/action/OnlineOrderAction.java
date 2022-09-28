package com.course.kafka.command.action;

import com.course.kafka.api.request.OnlineOrderRequest;
import com.course.kafka.broker.message.OnlineOrderMessage;
import com.course.kafka.broker.producer.OnlineOrderProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OnlineOrderAction {

    @Autowired
    private OnlineOrderProducer producer;

    public void publishToKafka(OnlineOrderRequest request) {
        OnlineOrderMessage message = new OnlineOrderMessage();

        message.setOnlineOrderNumber(request.getOnlineOrderNumber());
        message.setOrderDateTime(request.getOrderDateTime());
        message.setTotalAmount(request.getTotalAmount());
        message.setUsername(request.getUsername().toLowerCase());

        producer.publish(message);
    }

}

