package com.course.kafka.command.action;

import com.course.kafka.api.request.FlashSaleVoteRequest;
import com.course.kafka.broker.message.FlashSaleVoteMessage;
import com.course.kafka.broker.producer.FlashSaleVoteProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FlashSaleVoteAction {
    @Autowired
    private FlashSaleVoteProducer producer;

    public void publishToKafka(FlashSaleVoteRequest request) {
        FlashSaleVoteMessage message = new FlashSaleVoteMessage();

        message.setCustomerId(request.getCustomerId());
        message.setItemName(request.getItemName());

        producer.publish(message);
    }
}
