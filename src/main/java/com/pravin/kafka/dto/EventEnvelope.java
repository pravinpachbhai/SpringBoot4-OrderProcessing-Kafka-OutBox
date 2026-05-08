package com.pravin.kafka.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        Long aggregateId,
        String aggregateType,
        LocalDateTime timestamp,
        String payload
) {
}