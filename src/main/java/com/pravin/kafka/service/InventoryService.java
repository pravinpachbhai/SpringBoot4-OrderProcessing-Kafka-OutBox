package com.pravin.kafka.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.dto.InventoryResponse;
import com.pravin.kafka.entity.Inventory;
import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.entity.ProcessedEvent;
import com.pravin.kafka.event.InventoryReservedEvent;
import com.pravin.kafka.event.OrderCreatedEvent;
import com.pravin.kafka.exception.InsufficientStockException;
import com.pravin.kafka.exception.ResourceNotFoundException;
import com.pravin.kafka.repository.InventoryRepository;
import com.pravin.kafka.repository.OutboxRepository;
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
public class InventoryService {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private final InventoryRepository repo;
    private final DataMapper dataMapper;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;

    public InventoryService(InventoryRepository repo,
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

    @KafkaListener(topics = "order.created", groupId = "inventory-group")
    @Transactional(transactionManager = "transactionManager")
    public void handle(String message,
                       @Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_KEY) String key
    ) {


        log.info("order.created event received in inventory service to reserve the qty.{}", key);
        EventEnvelope<OrderCreatedEvent> eventEnvelope = null;
        OrderCreatedEvent event = null;

        try {
            eventEnvelope = objectMapper.readValue(
                    message,
                    new TypeReference<EventEnvelope<OrderCreatedEvent>>() {
                    }
            );
            event = eventEnvelope.payload();
            MDC.put("X-Correlation-Id", eventEnvelope.correlationId());
        } catch (Exception e) {
            log.error("Invalid event format", e);
            throw new RuntimeException(e);
        }


        try {
            UUID eventId = eventEnvelope.eventId();
            log.info("Event detail.{}", event);

            if (processedEventRepository.existsById(eventId)) {
                log.info("order.created event received but it was already processed");
                return;
            }
            event.items().forEach(item ->
                    reserve(item.productId(), item.quantity())
            );

            InventoryReservedEvent inventoryReservedEvent = new InventoryReservedEvent(event.id());

            UUID inventoryEventId = UUID.randomUUID();
            OutboxEvent outbox = new OutboxEvent();
            outbox.setId(inventoryEventId);
            outbox.setAggregateType("Order");
            outbox.setAggregateId(event.id());
            outbox.setEventType("inventory.reserved");
            outbox.setCorrelationId(eventEnvelope.correlationId());

            EventEnvelope<InventoryReservedEvent> envelope =
                    new EventEnvelope<>(
                            inventoryEventId,
                            eventEnvelope.correlationId(),
                            outbox.getEventType(),
                            outbox.getAggregateId(),
                            outbox.getAggregateType(),
                            LocalDateTime.now(),
                            inventoryReservedEvent
                    );

            try {
                outbox.setPayload(objectMapper.writeValueAsString(envelope));
            }catch (Exception e){
                 log.error("Error while setting the payload in inventory.", e);
                 throw new RuntimeException(e);
            }
            outbox.setStatus(OutboxEvent.Status.NEW);
            outbox.setCreatedAt(LocalDateTime.now());
            outboxRepository.save(outbox);
            log.info("Event publish for inventory.reserved.");

            try {
                processedEventRepository.save(
                        new ProcessedEvent(eventId, LocalDateTime.now())
                );
            } catch (DataIntegrityViolationException e) {
                log.info("Duplicate event ignored {}", eventEnvelope.eventId());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            MDC.clear();
        }

    }

    public InventoryResponse get(Long productId) {
        Inventory inventory =  repo.findById(productId)
                .orElseThrow(() -> new ResourceNotFoundException("Inventory not found"));
        return dataMapper.toResponse(inventory);
    }

    public InventoryResponse reserve(Long productId, int qty) {
        Inventory inventory =  repo.findById(productId)
                .orElseThrow(() -> new ResourceNotFoundException("Inventory not found"));

        if (inventory.getAvailableQuantity() < qty) {
            throw new InsufficientStockException("Not enough stock for product: " + productId);
        }

        inventory.setAvailableQuantity(inventory.getAvailableQuantity() - qty);
        inventory.setReservedQuantity(inventory.getReservedQuantity() + qty);

        return dataMapper.toResponse(repo.save(inventory));
    }

    public void release(Long productId, int qty) {
        Inventory inventory = repo.findById(productId)
                .orElseThrow(() -> new ResourceNotFoundException("Inventory not found"));
        inventory.setAvailableQuantity(inventory.getAvailableQuantity() + qty);
        inventory.setReservedQuantity(inventory.getReservedQuantity() - qty);
    }
}