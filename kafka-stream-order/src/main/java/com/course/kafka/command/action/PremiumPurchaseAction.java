package com.course.kafka.command.action;

import com.course.kafka.api.request.PremiumPurchaseRequest;
import com.course.kafka.broker.message.PremiumPurchaseMessage;
import com.course.kafka.broker.producer.PremiumPurchaseProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PremiumPurchaseAction {
    @Autowired
    private PremiumPurchaseProducer producer;

    public void publishToKafka(PremiumPurchaseRequest request) {
        PremiumPurchaseMessage message = new PremiumPurchaseMessage();

        message.setUsername(request.getUsername());
        message.setItem(request.getItem());
        message.setPurchaseNumber(request.getPurchaseNumber());

        producer.publish(message);
    }
}
