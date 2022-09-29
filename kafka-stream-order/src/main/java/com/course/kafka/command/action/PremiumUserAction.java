package com.course.kafka.command.action;

import com.course.kafka.api.request.PremiumUserRequest;
import com.course.kafka.broker.message.PremiumUserMessage;
import com.course.kafka.broker.producer.PremiumUserProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PremiumUserAction {
    @Autowired
    private PremiumUserProducer producer;

    public void publishToKafka(PremiumUserRequest request) {
        PremiumUserMessage message = new PremiumUserMessage();

        message.setUsername(request.getUsername());
        message.setLevel(request.getLevel());

        producer.publish(message);
    }
}
