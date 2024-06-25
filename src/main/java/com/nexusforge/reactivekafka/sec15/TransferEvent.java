package com.nexusforge.reactivekafka.sec15;

public record TransferEvent(
        String key,
        String from,
        String to,
        String amount,
        Runnable ackowledge
) {
}
