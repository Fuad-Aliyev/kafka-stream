package com.course.kafka.broker.stream.web;

import com.course.kafka.broker.message.WebColorVoteMessage;
import com.course.kafka.broker.message.WebDesignVoteMessage;
import com.course.kafka.broker.message.WebLayoutVoteMessage;
import com.course.kafka.util.WebColorVoteTimestampExtractor;
import com.course.kafka.util.WebLayoutVoteTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class WebDesignVoteInnerJoinStream {

    @Bean
    public KStream<String, WebDesignVoteMessage> kstreamWebDesignVote(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<WebColorVoteMessage> colorSerde = new JsonSerde<>(WebColorVoteMessage.class);
        JsonSerde<WebLayoutVoteMessage> layoutSerde = new JsonSerde<>(WebLayoutVoteMessage.class);
        JsonSerde<WebDesignVoteMessage> designSerde = new JsonSerde<>(WebDesignVoteMessage.class);

        builder
                .stream("t-commodity-web-vote-color",
                        Consumed.with(stringSerde, colorSerde, new WebColorVoteTimestampExtractor(), null)
                )
                .mapValues(v -> v.getColor())
                .to("t-commodity-web-vote-username-color");
        KTable<String, String> colorTable = builder.table("t-commodity-web-vote-username-color", Consumed.with(stringSerde, stringSerde));

        //we can convert stream to table directly or send to topic the stream then consume from that topic as table like in old versions
//        builder
//                .stream("t-commodity-web-vote-layout",
//                        Consumed.with(stringSerde, layoutSerde, new WebLayoutVoteTimestampExtractor(), null)
//                )
//                .mapValues(v -> v.getLayout())
//                .to("t-commodity-web-vote-username-layout");
//        KTable<String, String> layoutTable = builder.table("t-commodity-web-vote-username-layout", Consumed.with(stringSerde, stringSerde));

        KTable<String, String> layoutTable =
                builder
                .stream("t-commodity-web-vote-layout",
                        Consumed.with(stringSerde, layoutSerde, new WebLayoutVoteTimestampExtractor(), null)
                )
                .mapValues(v -> v.getLayout())
                .toTable();

        KTable<String, WebDesignVoteMessage> joinTable = colorTable.join(layoutTable, this::voteJoiner, Materialized.with(stringSerde, designSerde));
        joinTable.toStream().to("t-commodity-web-vote-result");

        joinTable.groupBy((username, voteDesign) -> KeyValue.pair(voteDesign.getColor(), voteDesign.getColor()))
                .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("Vote - color"));

        joinTable.groupBy((username, voteDesign) -> KeyValue.pair(voteDesign.getLayout(), voteDesign.getLayout()))
                .count().toStream().print(Printed.<String, Long>toSysOut().withLabel("Vote - layout"));

        return joinTable.toStream();
    }

    private WebDesignVoteMessage voteJoiner(String color, String layout) {
        WebDesignVoteMessage result = new WebDesignVoteMessage();

        result.setColor(color);
        result.setLayout(layout);

        return result;
    }
}
