package com.nexusforge.reactivekafka.sec13;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public class OrderEventProcessor {
    private static final Logger log = LoggerFactory.getLogger(OrderEventProcessor.class);
    private final ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer;

    public OrderEventProcessor(ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer) {
        this.deadLetterTopicProducer = deadLetterTopicProducer;
    }

    public Mono<Void> process(ReceiverRecord<String, String> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                   /* if (r.key().endsWith("5"))
                   // bug fixed
                        throw new RuntimeException("processing excpetion");*/
                    log.info("key: {}, value:{}", r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex -> new RecordProcessingExcpetion(record, ex))
                .transform(this.deadLetterTopicProducer.recordProcessingErrorHandler());
    }
}
