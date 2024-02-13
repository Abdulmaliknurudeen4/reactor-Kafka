package com.nexusforge.reactivekafka.sec01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Lec02KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(Lec02KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "inventory-service-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(Pattern.compile("order.*"));

        // uses - non deamon thread...
        KafkaReceiver.create(options).receive()
                .doOnNext(r -> log.info("topic: {}, key: {}, value:{}", r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                // commit interval.ms = 5000 ms. Ackowledge intervals
                .subscribe();
    }

    // if server shutsdown or network issues before acknowledgement whne
    // event has been processed, it can lead to unwanted scenarios like multiple
    // order charges in payment services

    //Acknowledgement is like a bookmark
}
