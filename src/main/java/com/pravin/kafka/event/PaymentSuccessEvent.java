package com.pravin.kafka.event;

import java.util.UUID;

public record PaymentSuccessEvent(Long orderId, UUID id) {
}