package com.pravin.kafka.dto;

import jakarta.validation.constraints.NotBlank;

public record AddressRequest(
        @NotBlank(message = "AddressLine is mandatory")
        String addressLine,
        @NotBlank(message = "City is mandatory")
        String city,
        @NotBlank(message = "State is mandatory")
        String state,
        @NotBlank(message = "Zip is mandatory")
        String zip
) {}