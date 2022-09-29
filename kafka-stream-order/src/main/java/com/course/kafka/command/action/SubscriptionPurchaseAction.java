package com.course.kafka.command.action;

import com.course.kafka.api.request.SubscriptionPurchaseRequest;
import com.course.kafka.broker.message.SubscriptionPurchaseMessage;
import com.course.kafka.broker.producer.SubscriptionPurchaseProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SubscriptionPurchaseAction {

    @Autowired
    private SubscriptionPurchaseProducer producer;

    public void publishToKafka(SubscriptionPurchaseRequest request) {
        SubscriptionPurchaseMessage message = new SubscriptionPurchaseMessage();

        message.setSubscriptionNumber(request.getSubscriptionNumber());
        message.setUsername(request.getUsername());

        producer.publish(message);
    }

}
