package com.pravin.kafka.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.dto.ShipmentRequest;
import com.pravin.kafka.dto.ShipmentResponse;
import com.pravin.kafka.entity.*;
import com.pravin.kafka.event.PaymentSuccessEvent;
import com.pravin.kafka.event.ShipmentCreatedEvent;
import com.pravin.kafka.repository.OutboxRepository;
import com.pravin.kafka.repository.ProcessedEventRepository;
import com.pravin.kafka.repository.ShipmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class ShippingService {
    private static final Logger log = LoggerFactory.getLogger(ShippingService.class);

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
    @Transactional(transactionManager = "transactionManager")
    public void ship(String message,
                     @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY) String key)  {
        log.info("payment.completed event received in shipping service for shipment.{}", key);
        EventEnvelope<PaymentSuccessEvent> eventEnvelope = null;
        PaymentSuccessEvent event = null;
        try {
            eventEnvelope = objectMapper.readValue(message, new TypeReference<>() {
            });
            event = eventEnvelope.payload();
            MDC.put("X-Correlation-Id", eventEnvelope.correlationId());
            UUID eventId = eventEnvelope.eventId();
            log.info("Event detail.{}", event);

            if (processedEventRepository.existsById(eventId)) {
                log.info("payment.completed event received but it was already processed");
                return; // already processed
            }

            if (shipmentCreated(event)) return;
            createOutboxEvent(event, eventEnvelope);
            try {
                processedEventRepository.save(
                        new ProcessedEvent(eventId, LocalDateTime.now())
                );
            } catch (DataIntegrityViolationException e) {
                log.info("Duplicate event ignored {}", eventId);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            MDC.clear();
        }
    }

    private boolean shipmentCreated(PaymentSuccessEvent event) {
        Shipment shipment = repo.findByOrderId(event.id())
                .orElseGet(() -> {
                    Shipment s = new Shipment();
                    s.setOrderId(event.id());
                    return s;
                });
        if (shipment.getStatus() == ShipmentStatus.CREATED) {
            log.info("Shippment already processed for order {}", shipment.getId());
            return true;
        }
        shipment.setTrackingNumber("TRACKING-12345");
        shipment.setStatus(ShipmentStatus.CREATED);
        repo.save(shipment);
        return false;
    }

    private void createOutboxEvent(PaymentSuccessEvent event, EventEnvelope<PaymentSuccessEvent> eventEnvelope) {
        ShipmentCreatedEvent shipmentCreatedEvent = new ShipmentCreatedEvent(event.id());
        UUID shippingEventId = UUID.randomUUID();
        OutboxEvent outbox = new OutboxEvent();
        outbox.setId(shippingEventId);
        outbox.setAggregateType("Order");
        outbox.setAggregateId(event.id());
        outbox.setEventType("shipment.created");
        outbox.setCorrelationId(eventEnvelope.correlationId());
        EventEnvelope<ShipmentCreatedEvent> envelope =
                new EventEnvelope<>(
                        shippingEventId,
                        eventEnvelope.correlationId(),
                        outbox.getEventType(),
                        outbox.getAggregateId(),
                        outbox.getAggregateType(),
                        LocalDateTime.now(),
                        shipmentCreatedEvent
                );

        try {
            outbox.setPayload(objectMapper.writeValueAsString(envelope));
        } catch (Exception e) {
            log.error("Error while setting the payload in payment create.", e);
            throw new RuntimeException(e);
        }
        outbox.setStatus(OutboxEvent.Status.NEW);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
        log.info("Event publish for shipment.created.");
    }

    public ShipmentResponse create(ShipmentRequest shipmentRequest) {
        Shipment shipment = dataMapper.toEntity(shipmentRequest);
        shipment.setStatus(ShipmentStatus.CREATED);
        return  dataMapper.toResponse(repo.save(shipment));
    }
}