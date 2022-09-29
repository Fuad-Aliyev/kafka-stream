package com.course.kafka.broker.stream.premium;

import com.course.kafka.broker.message.PremiumOfferMessage;
import com.course.kafka.broker.message.PremiumPurchaseMessage;
import com.course.kafka.broker.message.PremiumUserMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//@Configuration
public class PremiumOfferLeftJoinStream {
    @Bean
    public KStream<String, PremiumOfferMessage> kStreamPremiumOffer(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<PremiumPurchaseMessage> purchaseSerde = new JsonSerde<>(PremiumPurchaseMessage.class);
        JsonSerde<PremiumUserMessage> userSerde = new JsonSerde<>(PremiumUserMessage.class);
        JsonSerde<PremiumOfferMessage> offerSerde = new JsonSerde<>(PremiumOfferMessage.class);

        KStream<String, PremiumPurchaseMessage> purchaseStream =
                builder.stream("t-commodity-premium-purchase", Consumed.with(stringSerde, purchaseSerde))
                        .selectKey((k, v) -> v.getUsername());

        List<String> filterLevel = new ArrayList<>(Arrays.asList("gold", "diamond"));
        KTable<String, PremiumUserMessage> userTable =
                builder.table("t-commodity-premium-user", Consumed.with(stringSerde, userSerde))
                        .filter((k, v) -> v.getLevel() == null || filterLevel.contains(v.getLevel().toLowerCase()));

        KStream<String, PremiumOfferMessage> offerStream = purchaseStream.leftJoin(
                userTable,
                this::joiner,
                Joined.with(stringSerde, purchaseSerde, userSerde));

        offerStream.to("t-commodity-premium-offer", Produced.with(stringSerde, offerSerde));

        return offerStream;
    }

    private PremiumOfferMessage joiner(PremiumPurchaseMessage purchase, PremiumUserMessage user) {
        PremiumOfferMessage result = new PremiumOfferMessage();

        result.setUsername(purchase.getUsername());
        result.setPurchaseNumber(purchase.getPurchaseNumber());

        if (user != null) {
            result.setLevel(user.getLevel());
        } else {
            result.setLevel("Free");
        }

        return result;
    }
}
