package com.pravin.kafka.dto;

public record AddressResponse(
        Long id,
        String addressLine,
        String city,
        String state,
        String zip
) {}