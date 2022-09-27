package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingTwoMessage;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

public class FeedbackRatingTwoValueTransformer implements ValueTransformer<FeedbackMessage, FeedbackRatingTwoMessage> {
    private ProcessorContext processorContext;
    private final String stateStoreName;
    private KeyValueStore<String, FeedbackRatingTwoStoreValue> ratingStateStore;

    public FeedbackRatingTwoValueTransformer(String stateStoreName) {
        if (StringUtils.isEmpty(stateStoreName)) {
            throw new IllegalArgumentException("stateStoreName must not empty");
        }
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
        this.ratingStateStore = this.processorContext.getStateStore(stateStoreName);
    }

    @Override
    public FeedbackRatingTwoMessage transform(FeedbackMessage value) {
        FeedbackRatingTwoStoreValue storeValue = Optional.ofNullable(ratingStateStore.get(value.getLocation()))
                .orElse(new FeedbackRatingTwoStoreValue());
        Map<Integer, Long> ratingMap = Optional.ofNullable(storeValue.getRatingMap()).orElse(new TreeMap<>());
        Long currentRatingCount = Optional.ofNullable(ratingMap.get(value.getRating())).orElse(0L);
        long newRatingCount = currentRatingCount + 1;
        ratingMap.put(value.getRating(), newRatingCount);

        ratingStateStore.put(value.getLocation(), storeValue);
        FeedbackRatingTwoMessage branchRating = new FeedbackRatingTwoMessage();
        branchRating.setLocation(value.getLocation());
        branchRating.setRatingMap(ratingMap);
        branchRating.setAverageRating(calculateAverage(ratingMap));

        return null;
    }

    private double calculateAverage(Map<Integer, Long> ratingMap) {
        long sumRating = 0L;
        long countRating = 0L;

        for (Map.Entry<Integer, Long> entry : ratingMap.entrySet()) {
            sumRating += null;
        }
    }

    @Override
    public void close() {

    }
}
