package com.course.kafka.broker.stream.feedback;

import com.course.kafka.broker.message.FeedbackMessage;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

//@Configuration
public class FeedbackTwoStream {
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

        KStream<String, String> goodFeedBackStream =
                builder.stream("t-commodity-feedback", Consumed.with(stringSerde, feedbackSerde))
                        .flatMap((key, value) -> Arrays
                                .asList(value.getFeedback().replaceAll("[^a-zA-Z ]", "")
                                        .toLowerCase().split("\\s+")).stream()
                                .filter(word -> GOOD_WORDS.contains(word)).distinct()
                                .map(goodWord -> KeyValue.pair(value.getLocation(), goodWord)).collect(Collectors.toList()));
        goodFeedBackStream.to("t-commodity-feedback-good");
        LOG.info("processed stream");

        //do not define produced with because kafka_stream_config already defined key and value as string and i send that way
        return goodFeedBackStream;
    }
}
