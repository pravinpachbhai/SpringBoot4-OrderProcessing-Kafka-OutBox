package com.pravin.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.dto.PaymentRequest;
import com.pravin.kafka.dto.PaymentResponse;
import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.entity.Payment;
import com.pravin.kafka.entity.PaymentStatus;
import com.pravin.kafka.entity.ProcessedEvent;
import com.pravin.kafka.event.InventoryReservedEvent;
import com.pravin.kafka.repository.OutboxRepository;
import com.pravin.kafka.repository.PaymentRepository;
import com.pravin.kafka.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class PaymentService {
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private final PaymentRepository repo;
    private final DataMapper dataMapper;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;

    public PaymentService(PaymentRepository repo,
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

    @KafkaListener(topics = "inventory.reserved", groupId = "payment-group")
    @Transactional
    public void process(String message,
                        @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY) String key) throws JsonProcessingException {
        log.info("inventory.reserved event received in payment service to process payment.{}", key);

        EventEnvelope eventEnvelope = objectMapper.readValue(message, EventEnvelope.class);
        InventoryReservedEvent event = objectMapper.readValue(eventEnvelope.payload(), InventoryReservedEvent.class);
        log.info("Event detail.{}", event);

        if (processedEventRepository.existsById(eventEnvelope.eventId())) {
            log.info("inventory.reserved event received but it was already processed");
            return; // already processed
        }

        //TODO Call process method

        // simulate payment success
        OutboxEvent outbox = new OutboxEvent();
        outbox.setId(UUID.randomUUID());
        outbox.setAggregateType("Order");
        outbox.setAggregateId(String.valueOf(event.orderId()));
        outbox.setEventType("payment.completed");
        outbox.setPayload(message);
        outbox.setStatus(OutboxEvent.Status.NEW);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
        log.info("Event publish for payment.completed.");
        processedEventRepository.save(new ProcessedEvent(eventEnvelope.eventId(), LocalDateTime.now()));
    }

    public PaymentResponse process(PaymentRequest paymentRequest) {
        Payment payment = dataMapper.toEntity(paymentRequest);
        payment.setStatus(PaymentStatus.SUCCESS);
        return dataMapper.toResponse(repo.save(payment));
    }

    public void refundPayment(Long id) {
            //TODO
    }
}