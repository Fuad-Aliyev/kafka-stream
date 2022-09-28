package com.course.kafka.command.action;

import com.course.kafka.api.request.WebColorVoteRequest;
import com.course.kafka.broker.message.WebColorVoteMessage;
import com.course.kafka.broker.producer.WebColorVoteProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WebColorVoteAction {

    @Autowired
    private WebColorVoteProducer producer;

    public void publishToKafka(WebColorVoteRequest request) {
        WebColorVoteMessage message = new WebColorVoteMessage();

        message.setUsername(request.getUsername());
        message.setColor(request.getColor());
        message.setVoteDateTime(request.getVoteDateTime());

        producer.publish(message);
    }

}
