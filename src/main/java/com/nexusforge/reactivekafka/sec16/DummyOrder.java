package com.nexusforge.reactivekafka.sec16;

public record DummyOrder(
        String orderID,
        String customerId
) {
}
