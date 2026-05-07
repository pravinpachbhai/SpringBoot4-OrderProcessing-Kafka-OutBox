package com.pravin.kafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ShipmentRequest(
        @NotNull(message = "OrderId is mandatory")
        Long orderId
) {}