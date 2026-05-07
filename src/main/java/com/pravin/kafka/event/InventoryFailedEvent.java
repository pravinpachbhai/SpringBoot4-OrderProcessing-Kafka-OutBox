package com.pravin.kafka.event;

import java.util.UUID;

public record InventoryFailedEvent(Long orderId, String reason, UUID id) {
}