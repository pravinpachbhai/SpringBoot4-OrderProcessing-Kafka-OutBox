package com.pravin.kafka.event;

import java.util.UUID;

public record ShipmentCreatedEvent(Long orderId, UUID id) {
}