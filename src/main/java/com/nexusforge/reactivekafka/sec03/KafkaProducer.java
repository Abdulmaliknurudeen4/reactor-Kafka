package com.nexusforge.reactivekafka.sec03;

import com.nexusforge.reactivekafka.sec01.Lec02KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        var options = SenderOptions.<String, String>create(producerConfig);

        var map = Flux.range(1, 1_000_000)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        var start = System.currentTimeMillis();
        var sender = KafkaSender.create(options.maxInFlight(10_000));
        sender.send(map)
                .doOnNext(r -> log.info("Correlation Metadata id {}", r.correlationMetadata()))
                .doOnComplete(()->{
                    log.info("Total time take {} ms", System.currentTimeMillis() - start);
//                    sender.close();
                })
                .subscribe();

       /*var sender = KafkaSender.create(options);
       sender.close();*/
    }
}
