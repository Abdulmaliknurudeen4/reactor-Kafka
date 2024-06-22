package com.nexusforge.reactivekafka.sec14;

import com.nexusforge.reactivekafka.sec01.Lec02KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(Lec02KafkaConsumer.class);

    public static void main(String[] args) {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class
        );
        var options = SenderOptions.<String, Integer>create(producerConfig);

        var map = Flux.range(1,100)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), i))
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
