package com.nexusforge.reactivekafka.sec15;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.function.Predicate;

public class TransferEventProcessor {
    private static final Logger log = LoggerFactory.getLogger(TransferEventProcessor.class);
    private final KafkaSender<String, String> receiver;

    public TransferEventProcessor(KafkaSender<String, String> receiver) {
        this.receiver = receiver;
    }

    public void process(Flux<TransferEvent> flux) {

    }

    // 5 doesn't have money to transfer
    private Mono<TransferEvent> validate(TransferEvent event) {
        return Mono.just(event)
                .filter(Predicate.not(e -> e.key().equals("5")))
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(event.ackowledge())
                                .doFirst(() -> log.info("fails validateion : {}", event.key()))
                );
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecords(TransferEvent event){
        var pr1 = new ProducerRecord<>("transaction-events", event.key(), "%s+%s".formatted(event.to(), event.amount()));
        var pr2 = new ProducerRecord<>("transaction-events", event.key(), "%s-%s".formatted(event.from(), event.amount()));
        var sr1 = SenderRecord.create(pr1, pr1.key());
        var sr2 = SenderRecord.create(pr2, pr2.key());

        return Flux.just(sr1, sr2);
    }
}
