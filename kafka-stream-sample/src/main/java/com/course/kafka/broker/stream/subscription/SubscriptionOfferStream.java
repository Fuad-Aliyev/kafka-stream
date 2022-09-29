package com.course.kafka.broker.stream.subscription;

import com.course.kafka.broker.message.SubscriptionOfferMessage;
import com.course.kafka.broker.message.SubscriptionPurchaseMessage;
import com.course.kafka.broker.message.SubscriptionUserMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class SubscriptionOfferStream {

    @Bean
    public KStream<String, SubscriptionOfferMessage> kStreamSubscriptionOffer(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<SubscriptionPurchaseMessage> purchaseSerde = new JsonSerde<>(SubscriptionPurchaseMessage.class);
        JsonSerde<SubscriptionUserMessage> userSerde = new JsonSerde<>(SubscriptionUserMessage.class);
        JsonSerde<SubscriptionOfferMessage> offerSerde = new JsonSerde<>(SubscriptionOfferMessage.class);

        KStream<String, SubscriptionPurchaseMessage> purchaseStream =
                builder.stream("t-commodity-subscription-purchase", Consumed.with(stringSerde, purchaseSerde));
        KTable<String, SubscriptionUserMessage> userTable =
                builder.table("t-commodity-subscription-user", Consumed.with(stringSerde, userSerde));

        KStream<String, SubscriptionOfferMessage> offerStream = purchaseStream.join(
                userTable,
                this::joiner,
                Joined.with(stringSerde, purchaseSerde, userSerde)
        );

        offerStream.to("t-commodity-subscription-offer", Produced.with(stringSerde, offerSerde));
        return offerStream;
    }

    private SubscriptionOfferMessage joiner(SubscriptionPurchaseMessage purchase, SubscriptionUserMessage user) {
        SubscriptionOfferMessage result = new SubscriptionOfferMessage();

        result.setUsername(purchase.getUsername());
        result.setSubscriptionNumber(purchase.getSubscriptionNumber());

        result.setDuration(user.getDuration());

        return result;
    }
}
