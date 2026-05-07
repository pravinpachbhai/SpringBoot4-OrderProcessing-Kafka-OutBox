package com.pravin.kafka.dto;

public record ReserveInventoryRequest(
        Long productId,
        Integer quantity
) {}