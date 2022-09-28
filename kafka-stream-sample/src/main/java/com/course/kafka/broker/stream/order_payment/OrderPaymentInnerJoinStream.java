package com.course.kafka.broker.stream.order_payment;

import com.course.kafka.broker.message.OnlineOrderMessage;
import com.course.kafka.broker.message.OnlineOrderPaymentMessage;
import com.course.kafka.broker.message.OnlinePaymentMessage;
import com.course.kafka.util.OnlineOrderTimestampExtractor;
import com.course.kafka.util.OnlinePaymentTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

//@Configuration
public class OrderPaymentInnerJoinStream {

    @Bean
    public KStream<String, OnlineOrderMessage> kStreamOrderPayment(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<OnlineOrderMessage> orderSerde = new JsonSerde<>(OnlineOrderMessage.class);
        JsonSerde<OnlinePaymentMessage> paymentSerde = new JsonSerde<>(OnlinePaymentMessage.class);
        JsonSerde<OnlineOrderPaymentMessage> orderPaymentSerde = new JsonSerde<>(OnlineOrderPaymentMessage.class);

        KStream<String, OnlineOrderMessage> orderStream = builder
                .stream("t-commodity-online-order",
                        Consumed.with(stringSerde, orderSerde, new OnlineOrderTimestampExtractor(), null));

        KStream<String, OnlinePaymentMessage> paymentStream = builder
                .stream("t-commodity-online-payment",
                        Consumed.with(stringSerde, paymentSerde, new OnlinePaymentTimestampExtractor(), null));

        orderStream.join(
                paymentStream,
                this::joinOrderPayment,
                JoinWindows.of(Duration.ofHours(1L)).grace(Duration.ofMillis(0L)),
                StreamJoined.with(stringSerde, orderSerde, paymentSerde)
        )
        .to("t-commodity-join-order-payment", Produced.with(stringSerde, orderPaymentSerde));

        return orderStream;
    }

    private OnlineOrderPaymentMessage joinOrderPayment(OnlineOrderMessage order, OnlinePaymentMessage payment) {
        OnlineOrderPaymentMessage result = new OnlineOrderPaymentMessage();
        result.setOnlineOrderNumber(order.getOnlineOrderNumber());
        result.setOrderDateTime(order.getOrderDateTime());
        result.setTotalAmount(order.getTotalAmount());
        result.setUsername(order.getUsername());

        result.setPaymentDateTime(payment.getPaymentDateTime());
        result.setPaymentMethod(payment.getPaymentMethod());
        result.setPaymentNumber(payment.getPaymentNumber());

        return result;
    }
}
