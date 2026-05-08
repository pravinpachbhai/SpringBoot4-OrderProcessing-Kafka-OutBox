package com.pravin.kafka.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.dto.PaymentRequest;
import com.pravin.kafka.dto.PaymentResponse;
import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.entity.Payment;
import com.pravin.kafka.entity.PaymentStatus;
import com.pravin.kafka.entity.ProcessedEvent;
import com.pravin.kafka.event.InventoryReservedEvent;
import com.pravin.kafka.event.PaymentSuccessEvent;
import com.pravin.kafka.repository.OutboxRepository;
import com.pravin.kafka.repository.PaymentRepository;
import com.pravin.kafka.repository.ProcessedEventRepository;
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
    @Transactional(transactionManager = "transactionManager")
    public void process(String message,
                        @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY) String key)  {
        log.info("inventory.reserved event received in payment service to process payment.{}", key);
        EventEnvelope<InventoryReservedEvent> eventEnvelope = null;
        InventoryReservedEvent event = null;
        try {
            eventEnvelope = objectMapper.readValue(message, new TypeReference<>() {
            });
            event = eventEnvelope.payload();
            MDC.put("X-Correlation-Id", eventEnvelope.correlationId());
            UUID eventId = eventEnvelope.eventId();

            log.info("Event detail.{}", event);

            if (processedEventRepository.existsById(eventId)) {
            log.info("inventory.reserved event received but it was already processed");
            return; // already processed
        }

        //TODO Call process method

        PaymentSuccessEvent paymentSuccessEvent = new PaymentSuccessEvent(event.id());

        // simulate payment success
            UUID paymentEventId = UUID.randomUUID();
        OutboxEvent outbox = new OutboxEvent();
            outbox.setId(paymentEventId);
        outbox.setAggregateType("Order");
        outbox.setAggregateId(event.id());
        outbox.setEventType("payment.completed");
            outbox.setCorrelationId(eventEnvelope.correlationId());
            EventEnvelope<PaymentSuccessEvent> envelope =
                    new EventEnvelope<>(
                            paymentEventId,
                            eventEnvelope.correlationId(),
                            outbox.getEventType(),
                            outbox.getAggregateId(),
                            outbox.getAggregateType(),
                            LocalDateTime.now(),
                            paymentSuccessEvent
                    );
        try {
            outbox.setPayload(objectMapper.writeValueAsString(envelope));
        }catch (Exception e){
            log.error("Error while setting the payload in payment create.", e);
            throw new RuntimeException(e);
        }
        outbox.setStatus(OutboxEvent.Status.NEW);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
        log.info("Event publish for payment.completed.");
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

    public PaymentResponse process(PaymentRequest paymentRequest) {
        Payment payment = dataMapper.toEntity(paymentRequest);
        payment.setStatus(PaymentStatus.SUCCESS);
        return dataMapper.toResponse(repo.save(payment));
    }

    public void refundPayment(Long id) {
            //TODO
    }
}