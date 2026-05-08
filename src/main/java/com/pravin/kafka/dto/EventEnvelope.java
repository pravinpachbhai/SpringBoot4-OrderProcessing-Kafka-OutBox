package com.pravin.kafka.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public record EventEnvelope<T>(
        UUID eventId,
        String correlationId,
        String eventType,
        Long aggregateId,
        String aggregateType,
        LocalDateTime timestamp,
        T payload

) {
}