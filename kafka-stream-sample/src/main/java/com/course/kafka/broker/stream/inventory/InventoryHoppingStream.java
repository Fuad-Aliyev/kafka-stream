package com.course.kafka.broker.stream.inventory;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
public class InventoryHoppingStream {
    @Bean
    public KStream<String, InventoryMessage> kStreamInventory(StreamsBuilder builder) {
        Serde<String> stringSerde = Serdes.String();
        JsonSerde<InventoryMessage> inventorySerde = new JsonSerde<>(InventoryMessage.class);
        InventoryTimestampExtractor inventoryTimestampExtractor = new InventoryTimestampExtractor();
        Serde<Long> longSerde = Serdes.Long();

        Duration windowLength = Duration.ofHours(1L);
        Duration hopLength = Duration.ofMinutes(20L);
        Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis());

        KStream<String, InventoryMessage> inventoryStream = builder
                .stream("t-commodity-inventory",
                        Consumed.with(stringSerde, inventorySerde, inventoryTimestampExtractor, null));

        inventoryStream
                .mapValues((k, v) -> v.getType().equalsIgnoreCase("ADD") ? v.getQuantity() : (-1 * v.getQuantity()))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowLength).advanceBy(hopLength))
                .aggregate(() ->
                        0L, (aggKey, newValue, aggValue) -> aggValue + newValue, Materialized.with(stringSerde, longSerde)
                )
                .toStream()
                .through("t-commodity-inventory-three", Produced.with(windowedSerde, longSerde))
                .print(Printed.toSysOut());


        inventoryStream.to("t-commodity-inventory-three", Produced.with(stringSerde, inventorySerde));

        return inventoryStream;
    }
}
