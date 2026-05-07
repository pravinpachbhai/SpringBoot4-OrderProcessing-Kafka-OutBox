package com.pravin.kafka.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        String aggregateId,
        String aggregateType,
        LocalDateTime timestamp,
        String payload
) {
}