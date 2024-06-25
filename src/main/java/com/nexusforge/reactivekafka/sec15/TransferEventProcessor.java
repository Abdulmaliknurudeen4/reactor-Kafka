package com.nexusforge.reactivekafka.sec15;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.function.Predicate;

public class TransferEventProcessor {
    private static final Logger log = LoggerFactory.getLogger(TransferEventProcessor.class);
    private final KafkaSender<String, String> sender;

    public TransferEventProcessor(KafkaSender<String, String> receiver) {
        this.sender = receiver;
    }

    public Flux<SenderResult<String>> process(Flux<TransferEvent> flux) {
       return flux.concatMap(this::validate)
                .concatMap(this::sendTransaction);


    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent event){
        var senderRecords = this.toSenderRecords(event);
        var manager = this.sender.transactionManager();

        // transaction process blocks.
        return manager.begin()
                .then(
                        this.sender.send(senderRecords)
                                .concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.fromRunnable(event.ackowledge())))
                                .concatWith(manager.commit()).last())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> manager.abort());
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
        return Flux.just(sr1).concatWith(Mono.delay(Duration.ofSeconds(1)).then(Mono.error(new RuntimeException("oops"))))
                .concatWith(Mono.just(sr2));

        // use a random transaction ID for all transaction scenrios
        // if you stay longer that the transaction timeouts without
        // commit and aborting, you'll have producerFenced Exception
    }
}
