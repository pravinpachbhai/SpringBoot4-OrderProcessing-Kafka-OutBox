package com.pravin.kafka.dto;

import com.pravin.kafka.entity.ShipmentStatus;

import java.time.LocalDateTime;

public record ShipmentResponse(
        Long id,
        Long orderId,
        ShipmentStatus status,
        String trackingNumber,
        LocalDateTime createdAt
) {}