package com.nexusforge.reactivekafka.sec16;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderEvent(
        UUID orderID,
        long customerId,
        LocalDateTime orderDate
) {
}
