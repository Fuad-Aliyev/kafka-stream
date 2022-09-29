package com.course.kafka.util;

import com.course.kafka.broker.message.WebColorVoteMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WebColorVoteTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        WebColorVoteMessage message = (WebColorVoteMessage) record.value();
        return (message != null && message.getVoteDateTime() != null)
                ? LocalDateTimeUtil.toEpochTimestamp(message.getVoteDateTime())
                : record.timestamp();
    }
}
