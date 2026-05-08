package com.pravin.kafka.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private UUID id;

    private String aggregateType;
    private Long aggregateId;
    private String eventType;

    @Lob
    private String payload;

    @Enumerated(EnumType.STRING)
    private Status status;

    private LocalDateTime createdAt = null;
    private LocalDateTime updatedAt = null;
    private LocalDateTime processedAt = null;

    public enum Status {
        NEW, PUBLISHED, PENDING, PROCESSED, FAILED
    }
}