package com.pravin.kafka.dto;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record OrderRequest(
        @NotNull(message = "UserId is mandatory")
        Long userId,
        @NotEmpty(message = "At least one item is required")
        List<OrderItemRequest> items
) {}