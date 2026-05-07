package com.pravin.kafka.dto;

public record InventoryResponse(
        Long productId,
        Integer availableQuantity,
        Integer reservedQuantity
) {}