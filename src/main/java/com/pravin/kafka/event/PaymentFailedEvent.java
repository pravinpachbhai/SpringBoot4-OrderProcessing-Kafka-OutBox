package com.pravin.kafka.event;

import java.util.UUID;

public record PaymentFailedEvent(Long orderId, String reason, UUID id) {
}
