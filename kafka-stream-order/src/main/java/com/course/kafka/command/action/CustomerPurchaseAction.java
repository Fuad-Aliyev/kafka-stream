package com.course.kafka.command.action;

import com.course.kafka.api.request.CustomerPurchaseMobileRequest;
import com.course.kafka.api.request.CustomerPurchaseWebRequest;
import com.course.kafka.broker.message.CustomerPurchaseMobileMessage;
import com.course.kafka.broker.message.CustomerPurchaseWebMessage;
import com.course.kafka.broker.producer.CustomerPurchaseProducer;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CustomerPurchaseAction {
    @Autowired
    private CustomerPurchaseProducer producer;

    public String publishMobileToKafka(CustomerPurchaseMobileRequest request) {
        String purchaseNumber = "CP-MOBILE-" + RandomStringUtils.randomAlphanumeric(6).toUpperCase();
        CustomerPurchaseMobileMessage.Location location =
                new CustomerPurchaseMobileMessage.Location(
                        request.getLocation().getLatitude(),
                        request.getLocation().getLongitude()
                );

        CustomerPurchaseMobileMessage message =
                new CustomerPurchaseMobileMessage(
                        purchaseNumber, request.getPurchaseAmount(),
                    request.getMobileAppVersion(), request.getOperatingSystem(), location
                );

        producer.publishPurchaseMobile(message);

        return purchaseNumber;
    }

    public String publishWebToKafka(CustomerPurchaseWebRequest request) {
        String purchaseNumber = "CP-WEB-" + RandomStringUtils.randomAlphanumeric(6).toUpperCase();

        CustomerPurchaseWebMessage message =
                new CustomerPurchaseWebMessage(
                        purchaseNumber, request.getPurchaseAmount(), request.getBrowser(), request.getOperatingSystem()
                );

        producer.publishPurchaseWeb(message);

        return purchaseNumber;
    }
}
