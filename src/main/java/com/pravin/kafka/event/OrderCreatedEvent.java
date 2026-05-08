package com.pravin.kafka.event;

import java.util.List;

public record OrderCreatedEvent(Long id, List<OrderItemEvent> items) {
}