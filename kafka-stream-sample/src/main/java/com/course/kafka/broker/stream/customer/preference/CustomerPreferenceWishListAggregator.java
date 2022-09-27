package com.course.kafka.broker.stream.customer.preference;

import com.course.kafka.broker.message.CustomerPreferenceAggregateMessage;
import com.course.kafka.broker.message.CustomerPreferenceWishlistMessage;
import org.apache.kafka.streams.kstream.Aggregator;

public class CustomerPreferenceWishListAggregator implements
        Aggregator<String, CustomerPreferenceWishlistMessage, CustomerPreferenceAggregateMessage> {

    @Override
    public CustomerPreferenceAggregateMessage apply(String key,
                                                    CustomerPreferenceWishlistMessage value,
                                                    CustomerPreferenceAggregateMessage aggregate) {
        aggregate.putWishListItem(value.getItemName(), value.getWishlistDatetime());
        return aggregate;
    }
}
