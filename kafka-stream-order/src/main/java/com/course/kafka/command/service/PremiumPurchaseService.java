package com.course.kafka.command.service;

import com.course.kafka.api.request.PremiumPurchaseRequest;
import com.course.kafka.command.action.PremiumPurchaseAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PremiumPurchaseService {
    @Autowired
    private PremiumPurchaseAction action;

    public void createPurchase(PremiumPurchaseRequest request) {
        action.publishToKafka(request);
    }
}
