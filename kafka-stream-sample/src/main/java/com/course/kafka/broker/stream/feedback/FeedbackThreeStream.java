package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

//@Configuration
public class FeedbackThreeStream {
    private static final Set<String> GOOD_WORDS = new HashSet<>();
    private static final Set<String> BAD_WORDS = new HashSet<>();

    static {
        GOOD_WORDS.add("happy");
        GOOD_WORDS.add("good");
        GOOD_WORDS.add("helpful");

        BAD_WORDS.add("angry");
        BAD_WORDS.add("sad");
        BAD_WORDS.add("bad");
    }

    @Bean
    public KStream<String, FeedbackMessage> kStreamFeedback(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<FeedbackMessage> feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        KStream<String, FeedbackMessage> sourceStream =
                builder.stream("t-commodity-feedback", Consumed.with(stringSerde, feedbackSerde));
        sourceStream
                .flatMap(splitWords())
                .split()
                .branch(isGoodWord(), Branched.withConsumer(ks -> ks.to("t-commodity-feedback-good")))
                .branch(isBadWord(), Branched.withConsumer(ks -> ks.to("t-commodity-feedback-bad")));

        return sourceStream;
    }

    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> splitWords() {
        return (key, value) ->
                Arrays.asList(
                        value.getFeedback()
                        .replaceAll("[^a-zA-Z ]", "")
                        .toLowerCase()
                        .split("\\s+")
                )
                        .stream()
                        .distinct()
                        .map(word -> KeyValue.pair(value.getLocation(), word))
                        .collect(Collectors.toList());
    }

    private Predicate<String, String> isGoodWord() {
        return ((key, value) -> GOOD_WORDS.contains(value));
    }

    private Predicate<String, String> isBadWord() {
        return ((key, value) -> BAD_WORDS.contains(value));
    }
}
