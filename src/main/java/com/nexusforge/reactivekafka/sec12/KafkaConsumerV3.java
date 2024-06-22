package com.nexusforge.reactivekafka.sec12;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaConsumerV3 {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerV3.class);

    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group124",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        // uses - non deamon thread...
        KafkaReceiver.create(options).receive()
                .log()
                .concatMap(KafkaConsumerV3::process)
                .subscribe();
    }

    // if server shutsdown or network issues before acknowledgement whne
    // event has been processed, it can lead to unwanted scenarios like multiple
    // order charges in payment services

    //Acknowledgement is like a bookmark

    private static Mono<Void> process(ReceiverRecord<Object, Object> receiverRecord) {
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    if(r.key().toString().equals(5)){
                        throw new RuntimeException("DB is down");
                        // carries exception to the main pipeline if the
                        // database is down.
                    }
                    var index = ThreadLocalRandom.current().nextInt(1, 20);
                    log.info("key: {}, index{}, value:{}", r.key(), index, r.value().toString().toCharArray()[index]);
                    r.receiverOffset().acknowledge();
                })
                .retryWhen(retrySpec())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(IndexOutOfBoundsException.class,
                        ex->Mono.fromRunnable(()-> receiverRecord.receiverOffset()
                                        .acknowledge()))
                // if it's indexOut of Bound exception. Acknowledge after retrying and move on
                .then();


    }

    private static Retry retrySpec(){
        // retry only on a specfic kind of error
        return Retry.fixedDelay(3, Duration.ofSeconds(1))
                .filter(IndexOutOfBoundsException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
