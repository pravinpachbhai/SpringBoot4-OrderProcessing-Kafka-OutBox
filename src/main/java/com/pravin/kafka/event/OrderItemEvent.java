package com.pravin.kafka.event;

public record OrderItemEvent(
        Long productId,
        Integer quantity
) {
}