package com.nexusforge.reactivekafka.sec17.consumer;

public record DummyOrder(
        String orderID,
        String customerId
) {
}
