package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

//@Configuration
public class FeedbackStream {
    private static final Logger LOG = LoggerFactory.getLogger(FeedbackStream.class);
    private static final Set<String> GOOD_WORDS = new HashSet<>();

    static {
        GOOD_WORDS.add("happy");
        GOOD_WORDS.add("good");
        GOOD_WORDS.add("helpful");
    }

    @Bean
    public KStream<String, String> kStreamFeedback(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<FeedbackMessage> feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        KStream<String, String> goodFeedbackStream = builder.stream("t-commodity-feedback",
                        Consumed.with(stringSerde, feedbackSerde))
                .flatMapValues(mapperGoodWords());

        goodFeedbackStream.to("t-commodity-feedback-one-good", Produced.with(stringSerde, stringSerde));

        return goodFeedbackStream;
    }

    //gets feedback message as input and return iterable string
    private ValueMapper<FeedbackMessage, Iterable<String>> mapperGoodWords() {
        return feedbackMessage -> Arrays
                .asList(feedbackMessage.getFeedback().replaceAll("[^a-zA-Z ]", "")
                        .toLowerCase().split("\\s+")).stream()
                .filter(word -> GOOD_WORDS.contains(word)).distinct().collect(Collectors.toList());
    }
}
