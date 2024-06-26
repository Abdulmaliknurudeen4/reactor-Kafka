package com.nexusforge.reactivekafka.sec18;

import com.nexusforge.reactivekafka.sec01.Lec02KafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(Lec02KafkaConsumer.class);

    public static void main(String[] args) {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL",
                SaslConfigs.SASL_JAAS_CONFIG, """
                            org.apache.kafka.common.security.plain.PlainLoginModule required
                            username="admin"
                            password="secret"
                            user_admin="secret"
                            user_client="client-secret";
                    """,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Paths.get("src/main/resources/kafka.truststore.jks").toAbsolutePath().toString(),
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit"
        );
        var options = SenderOptions.<String, String>create(producerConfig);

        var map = Flux.interval(Duration.ofMillis(100)).
                take(100)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        var sender = KafkaSender.create(options);
        sender.send(map)
                .doOnNext(r -> log.info("Correlation Metadata id {}", r.correlationMetadata()))
                .doOnComplete(sender::close)
                .subscribe();

       /*var sender = KafkaSender.create(options);
       sender.close();*/
    }
}
