package com.nexusforge.reactivekafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Map;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"order-events"}
)
public abstract class AbstractIT {

    @Autowired
    private EmbeddedKafkaBroker broker;

    protected <K,V> KafkaReceiver<K,V> createReceiver(){
        var props = KafkaTestUtils
                .consumerProps("test-group", "true", broker);
        var options = ReceiverOptions.<K,V>create(props);
        return KafkaReceiver.create(options);
    }

}
