package com.pravin.kafka.dto;

import jakarta.validation.constraints.NotBlank;

public record NotificationRequest(
        @NotBlank(message = "Type is mandatory")
        String type,
        @NotBlank(message = "Recipient is mandatory")
        String recipient,
        @NotBlank(message = "Message is mandatory")
        String message
) {}
