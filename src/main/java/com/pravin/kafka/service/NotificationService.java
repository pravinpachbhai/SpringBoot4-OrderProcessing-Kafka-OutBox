package com.pravin.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.dto.NotificationRequest;
import com.pravin.kafka.dto.NotificationResponse;
import com.pravin.kafka.entity.Notification;
import com.pravin.kafka.entity.ProcessedEvent;
import com.pravin.kafka.event.PaymentSuccessEvent;
import com.pravin.kafka.event.ShipmentCreatedEvent;
import com.pravin.kafka.repository.NotificationRepository;
import com.pravin.kafka.repository.OutboxRepository;
import com.pravin.kafka.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Service
public class NotificationService {
    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository repo;
    private final DataMapper dataMapper;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;

    public NotificationService(NotificationRepository repo,
                               DataMapper dataMapper,
                               OutboxRepository outboxRepository,
                               ObjectMapper objectMapper,
                               ProcessedEventRepository processedEventRepository) {
        this.repo = repo;
        this.dataMapper = dataMapper;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.processedEventRepository = processedEventRepository;
    }

    @KafkaListener(topics = "shipment.created", groupId = "notification-group")
    public void notify(String message,
                       @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY) String key) throws JsonProcessingException {
        log.info("shipment.created event received in notification service to inform customer for Order Id.{}", key);

        EventEnvelope eventEnvelope = objectMapper.readValue(message, EventEnvelope.class);
        ShipmentCreatedEvent event = objectMapper.readValue(eventEnvelope.payload(), ShipmentCreatedEvent.class);
        log.info("Event detail.{}", event);
        if (processedEventRepository.existsById(eventEnvelope.eventId())) {
            log.info("shipment.created event received but it was already processed");
            return; // already processed
        }
        processedEventRepository.save(new ProcessedEvent(eventEnvelope.eventId(), LocalDateTime.now()));
        log.info("Email sent for order: " + event.orderId());
    }

    public NotificationResponse send(NotificationRequest notificationRequest) {
        Notification notification = dataMapper.toEntity(notificationRequest);
        notification.setStatus("SENT");
        return dataMapper.toResponse(repo.save(notification));
    }
}