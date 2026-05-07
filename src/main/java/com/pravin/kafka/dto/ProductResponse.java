package com.pravin.kafka.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record ProductResponse(
    Long id,
    String code,
    String name,
    BigDecimal price,
    LocalDateTime created,
    LocalDateTime updated
){}
