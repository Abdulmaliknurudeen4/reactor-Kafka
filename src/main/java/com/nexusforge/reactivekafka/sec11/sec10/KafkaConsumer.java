package com.nexusforge.reactivekafka.sec11.sec10;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static reactor.core.scheduler.Schedulers.boundedElastic;

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events"));

        // uses - non deamon thread...
        KafkaReceiver.create(options)
                .receive()
                .groupBy(r -> Integer.parseInt(r.key()) % 5)
                .flatMap(KafkaConsumer::batchProcess)
                .log()
                .subscribe();
    }

    private static Mono<Void> batchProcess(GroupedFlux<Integer,
            ReceiverRecord<String, String>> flux) {
        return flux
                .publishOn(boundedElastic())
                .doFirst(() -> log.info("==========mod: {}", flux.key()))
                .doOnNext(r -> log.info("key: {}, value:{}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .then(Mono.delay(Duration.ofSeconds(1))).then();

        //flatmap has ordering issues when messages are split
        // over fluxes
    }
}
