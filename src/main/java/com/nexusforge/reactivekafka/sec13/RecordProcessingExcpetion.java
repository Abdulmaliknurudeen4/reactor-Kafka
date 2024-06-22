package com.nexusforge.reactivekafka.sec13;

import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingExcpetion extends RuntimeException {
    private final ReceiverRecord<?, ?> record;

    public RecordProcessingExcpetion(ReceiverRecord<?, ?> record, Throwable e) {

        super(e);

        this.record = record;
    }

    @SuppressWarnings("unchecked")
    public <K, V> ReceiverRecord<K, V> getRecord() {
        return (ReceiverRecord<K, V>) record;
    }
}
