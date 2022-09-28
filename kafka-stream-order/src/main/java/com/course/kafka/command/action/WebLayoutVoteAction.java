package com.course.kafka.command.action;

import com.course.kafka.api.request.WebLayoutVoteRequest;
import com.course.kafka.broker.message.WebLayoutVoteMessage;
import com.course.kafka.broker.producer.WebLayoutVoteProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WebLayoutVoteAction {
    @Autowired
    private WebLayoutVoteProducer producer;

    public void publishToKafka(WebLayoutVoteRequest request) {
        WebLayoutVoteMessage message = new WebLayoutVoteMessage();

        message.setUsername(request.getUsername());
        message.setLayout(request.getLayout());
        message.setVoteDateTime(request.getVoteDateTime());

        producer.publish(message);
    }
}
