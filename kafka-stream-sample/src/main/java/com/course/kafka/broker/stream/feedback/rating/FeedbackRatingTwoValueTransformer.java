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

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
        this.ratingStateStore = (KeyValueStore<String, FeedbackRatingTwoStoreValue>) this.processorContext
                .getStateStore(stateStoreName);
    }

    @Override
    public FeedbackRatingTwoMessage transform(FeedbackMessage value) {
        FeedbackRatingTwoStoreValue storeValue = Optional.ofNullable(ratingStateStore.get(value.getLocation()))
                .orElse(new FeedbackRatingTwoStoreValue());
        Map<Integer, Long> ratingMap = Optional.ofNullable(storeValue.getRatingMap()).orElse(new TreeMap<>());

        Long currentRatingCount = Optional.ofNullable(ratingMap.get(value.getRating())).orElse(0l);
        Long newRatingCount = currentRatingCount + 1;

        ratingMap.put(value.getRating(), newRatingCount);
        ratingStateStore.put(value.getLocation(), storeValue);

        FeedbackRatingTwoMessage branchRating = new FeedbackRatingTwoMessage();
        branchRating.setLocation(value.getLocation());
        branchRating.setRatingMap(ratingMap);
        branchRating.setAverageRating(calculateAverage(ratingMap));

        return branchRating;
    }

    private double calculateAverage(Map<Integer, Long> ratingMap) {
        Long sumRating = 0l;
        Long countRating = 0l;

        for (Map.Entry<Integer, Long> entry : ratingMap.entrySet()) {
            sumRating += entry.getKey() * entry.getValue();
            countRating += entry.getValue();
        }

        return Math.round((double) sumRating / countRating * 10d) / 10d;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }
}
