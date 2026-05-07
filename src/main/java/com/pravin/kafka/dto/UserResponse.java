package com.pravin.kafka.dto;

import java.time.LocalDateTime;
import java.util.List;

public record UserResponse(
        Long id,
        String name,
        String email,
        LocalDateTime createdAt,
        List<AddressResponse> addresses
) {}