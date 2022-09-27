package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderPatternMessage;
import com.course.kafka.broker.message.OrderRewardMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class CommodityFraudFurtherStream {
    private static final Logger LOG = LoggerFactory.getLogger(CommodityFraudFurtherStream.class);

    @Bean
    public KStream<String, OrderMessage> kstreamCommodityTrading(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<OrderMessage> orderSerde = new JsonSerde<>(OrderMessage.class);
        JsonSerde<OrderPatternMessage> orderPatternSerde = new JsonSerde<>(OrderPatternMessage.class);
        JsonSerde<OrderRewardMessage> orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);

        KStream<String, OrderMessage> maskedCreditCardStream =
                builder.stream("t-commodity-order", Consumed.with(stringSerde, orderSerde))
                        .mapValues(CommodityStreamUtil::maskCreditCard);

        //DO PATTERN PROCESS
        //new version of branching
        maskedCreditCardStream.mapValues(CommodityStreamUtil::mapToOrderPattern)
                .split()
                .branch(
                        CommodityStreamUtil.isPlastic(),
                        Branched.withConsumer(ks -> ks.to("t-commodity-pattern-two-plastic",
                                Produced.with(stringSerde, orderPatternSerde)))
                )
                .branch((key, value) -> true,
                        Branched.withConsumer(ks -> ks.to("t-commodity-pattern-two-notplastic",
                                Produced.with(stringSerde, orderPatternSerde)))
                );


        //DO REWARD PROCESS
        KStream<String, OrderRewardMessage> rewardStream = maskedCreditCardStream
                .filter(CommodityStreamUtil.isLargeQuantity())
                .filterNot(CommodityStreamUtil.isCheap())
                .map(CommodityStreamUtil.mapToOrderRewardChangeKey());
        rewardStream.to("t-commodity-reward-two", Produced.with(stringSerde, orderRewardSerde));

        //DO STORAGE PROCESS
        KStream<String, OrderMessage> storageStream = maskedCreditCardStream.selectKey(CommodityStreamUtil.generateStorageKey());
        storageStream.to("t-commodity-storage-two", Produced.with(stringSerde, orderSerde));

        //fraud sink
        KStream<String, Integer> fraudStream = maskedCreditCardStream
                .filter((key, value) -> value.getOrderLocation().startsWith("C"))
                .peek((key, value) -> this.reportFraud(value))
                .map((key, value) ->
                        KeyValue.pair(
                                value.getOrderLocation().toUpperCase().charAt(0) + "***",
                                value.getPrice() * value.getQuantity()
                        )
                );
        fraudStream.to("t-commodity-fraud", Produced.with(Serdes.String(), Serdes.Integer()));
        return maskedCreditCardStream;
    }

    private void reportFraud(OrderMessage value) {
        LOG.info("Reporting fraud : {}", value);
    }
}
