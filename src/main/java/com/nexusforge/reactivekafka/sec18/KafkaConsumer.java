package com.nexusforge.reactivekafka.sec18;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group124",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL",
                SaslConfigs.SASL_JAAS_CONFIG, """
                            org.apache.kafka.common.security.plain.PlainLoginModule required
                            username="admin"
                            password="secret"
                            user_admin="secret"
                            user_client="client-secret";
                    """
//                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Paths.get("src/main/resources/kafka.truststore.jks").toAbsolutePath().toString(),
//                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        // uses - non deamon thread...
        KafkaReceiver.create(options).receive()
                .doOnNext(r -> log.info("key: {}, value:{}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                // commit interval.ms = 5000 ms. Ackowledge intervals
                .subscribe();
    }

    // if server shutsdown or network issues before acknowledgement whne
    // event has been processed, it can lead to unwanted scenarios like multiple
    // order charges in payment services

    //Acknowledgement is like a bookmark
}
