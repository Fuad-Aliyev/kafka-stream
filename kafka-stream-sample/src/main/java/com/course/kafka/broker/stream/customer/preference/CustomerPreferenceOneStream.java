package com.course.kafka.broker.stream.customer.preference;

import com.course.kafka.broker.message.CustomerPreferenceAggregateMessage;
import com.course.kafka.broker.message.CustomerPreferenceShoppingCartMessage;
import com.course.kafka.broker.message.CustomerPreferenceWishlistMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class CustomerPreferenceOneStream {
    private static final CustomerPreferenceWishListAggregator WISH_LIST_AGGREGATOR =
            new CustomerPreferenceWishListAggregator();
    private static final CustomerPreferenceShoppingCartAggregator SHOPPING_CART_AGGREGATOR =
            new CustomerPreferenceShoppingCartAggregator();

    @Bean
    public KStream<String, CustomerPreferenceAggregateMessage> kstreamCustomerPreferenceAll(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<CustomerPreferenceWishlistMessage> wishListSerde =
                new JsonSerde<>(CustomerPreferenceWishlistMessage.class);
        JsonSerde<CustomerPreferenceShoppingCartMessage> shoppingCartSerde =
                new JsonSerde<>(CustomerPreferenceShoppingCartMessage.class);
        JsonSerde<CustomerPreferenceAggregateMessage> aggregateSerde =
                new JsonSerde<>(CustomerPreferenceAggregateMessage.class);

        KGroupedStream<String, CustomerPreferenceWishlistMessage> groupedWishListStream =
                builder.stream(
                                "t-commodity-customer-preference-wishlist",
                                Consumed.with(stringSerde, wishListSerde)
                        )
                        .groupByKey();

        KGroupedStream<String, CustomerPreferenceShoppingCartMessage> groupedShoppingCardStream =
                builder.stream(
                        "t-commodity-customer-preference-shopping-cart",
                                Consumed.with(stringSerde, shoppingCartSerde)
                )
                .groupByKey();


        KStream<String, CustomerPreferenceAggregateMessage> customerPreferenceStream =
                groupedWishListStream
                    .cogroup(WISH_LIST_AGGREGATOR)
                    .cogroup(groupedShoppingCardStream, SHOPPING_CART_AGGREGATOR)
                    .aggregate(() -> new CustomerPreferenceAggregateMessage(), Materialized.with(stringSerde, aggregateSerde))
                    .toStream();
        customerPreferenceStream.to("t-commodity-customer-preference-all", Produced.with(stringSerde, aggregateSerde));

        return customerPreferenceStream;
    }
}
