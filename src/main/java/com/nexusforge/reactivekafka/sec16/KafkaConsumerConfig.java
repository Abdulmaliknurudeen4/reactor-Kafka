package com.nexusforge.reactivekafka.sec16;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, OrderEvent> receiverOptions(KafkaProperties kafkaProperties) {
        return ReceiverOptions.<String, OrderEvent>create(kafkaProperties.buildConsumerProperties(null))
                .subscription(List.of("order-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, OrderEvent> consumerTemplate(ReceiverOptions<String, OrderEvent> options) {
        return new ReactiveKafkaConsumerTemplate<>(options);
    }
}
