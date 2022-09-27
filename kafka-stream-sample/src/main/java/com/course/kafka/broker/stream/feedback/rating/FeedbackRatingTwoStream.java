package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingTwoMessage;
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

@Configuration
public class FeedbackRatingTwoStream {

    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedbackRating(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<FeedbackMessage> feedbackSerde = new JsonSerde<>(FeedbackMessage.class);
        JsonSerde<FeedbackRatingTwoMessage> feedbackRatingTwoSerde = new JsonSerde<>(FeedbackRatingTwoMessage.class);
        JsonSerde<FeedbackRatingTwoStoreValue> feedbackRatingTwoStoreValueSerde =
                new JsonSerde<>(FeedbackRatingTwoStoreValue.class);

        KStream<String, FeedbackMessage> feedbackStream = builder
                .stream("t-commodity-feedback", Consumed.with(stringSerde, feedbackSerde));

        String feedbackRatingStateStoreName = "feedbackRatingTwoStateStore";
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(feedbackRatingStateStoreName);
        StoreBuilder<KeyValueStore<String, FeedbackRatingTwoStoreValue>> storeBuilder =
                Stores.keyValueStoreBuilder(storeSupplier, stringSerde, feedbackRatingTwoStoreValueSerde);

        builder.addStateStore(storeBuilder);

        feedbackStream
                .transformValues(() -> new FeedbackRatingTwoValueTransformer(feedbackRatingStateStoreName),
                        feedbackRatingStateStoreName)
                .to("t-commodity-feedback-rating-two", Produced.with(stringSerde, feedbackRatingTwoSerde));

        return feedbackStream;
    }
}
