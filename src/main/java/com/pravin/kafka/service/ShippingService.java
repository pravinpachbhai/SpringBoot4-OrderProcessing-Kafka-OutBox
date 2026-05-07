package com.pravin.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.dto.ShipmentRequest;
import com.pravin.kafka.dto.ShipmentResponse;
import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.entity.ProcessedEvent;
import com.pravin.kafka.entity.Shipment;
import com.pravin.kafka.entity.ShipmentStatus;
import com.pravin.kafka.event.InventoryReservedEvent;
import com.pravin.kafka.event.PaymentSuccessEvent;
import com.pravin.kafka.event.ShipmentCreatedEvent;
import com.pravin.kafka.repository.OutboxRepository;
import com.pravin.kafka.repository.ProcessedEventRepository;
import com.pravin.kafka.repository.ShipmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class ShippingService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final ShipmentRepository repo;
    private final DataMapper dataMapper;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;

    public ShippingService(ShipmentRepository repo,
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

    @KafkaListener(topics = "payment.completed", groupId = "shipping-group")
    @Transactional
    public void ship(String message,
                     @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY) String key) throws JsonProcessingException {
        log.info("payment.completed event received in shipping service for shipment.{}", key);

        EventEnvelope eventEnvelope = objectMapper.readValue(message, EventEnvelope.class);
        PaymentSuccessEvent event = objectMapper.readValue(eventEnvelope.payload(), PaymentSuccessEvent.class);
        log.info("Event detail.{}", event);

        if (processedEventRepository.existsById(eventEnvelope.eventId())) {
            log.info("payment.completed event received but it was already processed");
            return; // already processed
        }

        //TODO Call create method

        // simulate shipping success
        OutboxEvent outbox = new OutboxEvent();
        outbox.setId(UUID.randomUUID());
        outbox.setAggregateType("Order");
        outbox.setAggregateId(String.valueOf(event.orderId()));
        outbox.setEventType("shipment.created");
        outbox.setPayload(message);
        outbox.setStatus(OutboxEvent.Status.NEW);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
        log.info("Event publish for shipment.created.");
        processedEventRepository.save(new ProcessedEvent(eventEnvelope.eventId(), LocalDateTime.now()));

    }

    public ShipmentResponse create(ShipmentRequest shipmentRequest) {
        Shipment shipment = dataMapper.toEntity(shipmentRequest);
        shipment.setStatus(ShipmentStatus.CREATED);
        return  dataMapper.toResponse(repo.save(shipment));
    }
}