package com.pravin.kafka.event;

import java.util.List;
import java.util.UUID;

public record OrderCreatedEvent(
        Long orderId,
        Long userId,
        List<OrderItemEvent> items,
        UUID id
) {
}