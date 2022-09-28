package com.course.kafka.command.service;

import com.course.kafka.api.request.OnlineOrderRequest;
import com.course.kafka.command.action.OnlineOrderAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OnlineOrderService {

    @Autowired
    private OnlineOrderAction action;

    public void saveOnlineOrder(OnlineOrderRequest request) {
        action.publishToKafka(request);
    }

}

