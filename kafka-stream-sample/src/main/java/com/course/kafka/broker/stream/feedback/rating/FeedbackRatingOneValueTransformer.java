package com.course.kafka.broker.stream.feedback.rating;

import com.course.kafka.broker.message.FeedbackMessage;
import com.course.kafka.broker.message.FeedbackRatingOneMessage;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.util.StringUtils;

import java.util.Optional;

public class FeedbackRatingOneValueTransformer implements ValueTransformer<FeedbackMessage, FeedbackRatingOneMessage> {
    private ProcessorContext processorContext;

    private final String stateStoreName;

    private KeyValueStore<String, FeedbackRatingOneStoreValue> ratingStateStore;

    public FeedbackRatingOneValueTransformer(String stateStoreName) {
        if (StringUtils.isEmpty(stateStoreName)) {
            throw new IllegalArgumentException("stateStoreName must not empty");
        }

        this.stateStoreName = stateStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
        this.ratingStateStore = (KeyValueStore<String, FeedbackRatingOneStoreValue>) this.processorContext
                .getStateStore(stateStoreName);
    }

    @Override
    public FeedbackRatingOneMessage transform(FeedbackMessage value) {
        FeedbackRatingOneStoreValue storeValue = Optional.ofNullable(ratingStateStore.get(value.getLocation()))
                .orElse(new FeedbackRatingOneStoreValue());

        long newSumRating = storeValue.getSumRating() + value.getRating();
        storeValue.setSumRating(newSumRating);

        long newCountRating = storeValue.getCountRating() + 1;
        storeValue.setCountRating(newCountRating);

        ratingStateStore.put(value.getLocation(), storeValue);

        FeedbackRatingOneMessage branchRating = new FeedbackRatingOneMessage();
        branchRating.setLocation(value.getLocation());

        double averageRating = Math.round((double) newSumRating / newCountRating * 10d) / 10d;
        branchRating.setAverageRating(averageRating);

        return branchRating;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }
}
