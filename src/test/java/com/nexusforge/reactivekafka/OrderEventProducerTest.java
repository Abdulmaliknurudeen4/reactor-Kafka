package com.nexusforge.reactivekafka;

import com.nexusforge.reactivekafka.sec17.producer.OrderEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;

import java.time.Duration;

@TestPropertySource(properties = "app=producer")
public class OrderEventProducerTest extends AbstractIT{
    private static final Logger log = LoggerFactory.getLogger(OrderEventProducerTest.class);
    
    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    public void producerTest1(){
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-events");
        var orderEvent = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: {}, value:{}", r.key(), r.value()));

        StepVerifier.create(orderEvent)
                .consumeNextWith(r -> Assertions.assertNotNull(r.value().orderID()))
                .expectNextCount(9)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
    public void producerTest2(){
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-events");
        var orderEvent = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: {}, value:{}", r.key(), r.value()));

        StepVerifier.create(orderEvent)
                .consumeNextWith(r -> Assertions.assertNotNull(r.value().orderID()))
                .expectNextCount(9)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    public void producerTest3(){
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-events");
        var orderEvent = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: {}, value:{}", r.key(), r.value()));

        StepVerifier.create(orderEvent)
                .consumeNextWith(r -> Assertions.assertNotNull(r.value().orderID()))
                .expectNextCount(9)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }
}
