package com.course.kafka.broker.stream.commodity;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderPatternMessage;
import com.course.kafka.broker.message.OrderRewardMessage;
import com.course.kafka.util.CommodityStreamUtil;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;

//@Configuration
public class CommodityOneStream {

    @Bean
    public KStream<String, OrderMessage> kstreamCommodityTrading(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<OrderMessage> orderSerde = new JsonSerde<>(OrderMessage.class);
        JsonSerde<OrderPatternMessage> orderPatternSerde = new JsonSerde<>(OrderPatternMessage.class);
        JsonSerde<OrderRewardMessage> orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);

        KStream<String, OrderMessage> maskedCreditCardStream =
                builder.stream("t-commodity-order", Consumed.with(stringSerde, orderSerde))
                        .mapValues(CommodityStreamUtil::maskCreditCard);
        KStream<String, OrderPatternMessage> patternStream =
                maskedCreditCardStream.mapValues(CommodityStreamUtil::mapToOrderPattern);
        patternStream.to("t-commodity-pattern-one", Produced.with(stringSerde, orderPatternSerde));

        KStream<String, OrderRewardMessage> rewardStream =
                maskedCreditCardStream.filter(CommodityStreamUtil.isLargeQuantity())
                .mapValues(CommodityStreamUtil::mapToOrderReward);
        rewardStream.to("t-commodity-reward-one", Produced.with(stringSerde, orderRewardSerde));
        maskedCreditCardStream.to("t-commodity-storage-one", Produced.with(stringSerde, orderSerde));
        return maskedCreditCardStream;
    }
}
