package com.course.kafka.command.service;

import com.course.kafka.api.request.SubscriptionPurchaseRequest;
import com.course.kafka.command.action.SubscriptionPurchaseAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SubscriptionPurchaseService {

    @Autowired
    private SubscriptionPurchaseAction action;

    public void createPurchase(SubscriptionPurchaseRequest request) {
        action.publishToKafka(request);
    }

}
