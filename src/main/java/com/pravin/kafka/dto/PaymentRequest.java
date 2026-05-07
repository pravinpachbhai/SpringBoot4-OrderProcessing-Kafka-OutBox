package com.pravin.kafka.dto;

import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record PaymentRequest(
        @NotNull(message = "OrderId is mandatory")
        Long orderId,
        @NotNull(message = "Amount is mandatory")
        BigDecimal amount
) {}

