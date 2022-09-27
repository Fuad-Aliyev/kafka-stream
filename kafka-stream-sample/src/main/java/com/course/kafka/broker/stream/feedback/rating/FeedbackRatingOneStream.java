package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingOneMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class FeedbackRatingOneStream {

    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedbackRating(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<FeedbackMessage> feedbackSerde = new JsonSerde<>(FeedbackMessage.class);
        JsonSerde<FeedbackRatingOneMessage> feedbackRatingOneSerde = new JsonSerde<>(FeedbackRatingOneMessage.class);
        JsonSerde<FeedbackRatingOneStoreValue> feedbackRatingOneStoreValueSerde = new JsonSerde<>(FeedbackRatingOneStoreValue.class);

        KStream<String, FeedbackMessage> feedbackStream = builder.stream("t-commodity-feedback", Consumed.with(stringSerde, feedbackSerde));

        String feedbackRatingStateStoreName = "feedbackRatingOneStateStore";
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(feedbackRatingStateStoreName);
        StoreBuilder<KeyValueStore<String, FeedbackRatingOneStoreValue>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, stringSerde, feedbackRatingOneStoreValueSerde);

        builder.addStateStore(storeBuilder);

        feedbackStream
                .transformValues(() -> new FeedbackRatingOneValueTransformer(feedbackRatingStateStoreName),
                        feedbackRatingStateStoreName)
                .to("t-commodity-feedback-rating-one", Produced.with(stringSerde, feedbackRatingOneSerde));
        return feedbackStream;
    }
}
