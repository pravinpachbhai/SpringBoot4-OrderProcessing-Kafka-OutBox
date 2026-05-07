package com.pravin.kafka.event;

import java.util.UUID;

public record OrderItemEvent(
        Long productId,
        Integer quantity,
        UUID id

) {
}