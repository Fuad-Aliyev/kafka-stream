package com.course.kafka.command.action;

import com.course.kafka.api.request.CustomerPreferenceShoppingCartRequest;
import com.course.kafka.api.request.CustomerPreferenceWishlistRequest;
import com.course.kafka.broker.message.CustomerPreferenceShoppingCartMessage;
import com.course.kafka.broker.message.CustomerPreferenceWishlistMessage;
import com.course.kafka.broker.producer.CustomerPreferenceProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class CustomerPreferenceAction {
    @Autowired
    private CustomerPreferenceProducer producer;

    public void publishShoppingCart(CustomerPreferenceShoppingCartRequest request) {
        CustomerPreferenceShoppingCartMessage message =
                new CustomerPreferenceShoppingCartMessage(request.getCustomerId(), request.getItemName(),
                request.getCartAmount(), LocalDateTime.now());

        producer.publishShoppingCart(message);
    }

    public void publishWishlist(CustomerPreferenceWishlistRequest request) {
        CustomerPreferenceWishlistMessage message =
                new CustomerPreferenceWishlistMessage(request.getCustomerId(), request.getItemName(), LocalDateTime.now());

        producer.publishWishlist(message);
    }
}
