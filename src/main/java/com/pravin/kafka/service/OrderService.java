package com.pravin.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pravin.kafka.component.DataMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.dto.OrderRequest;
import com.pravin.kafka.dto.OrderResponse;
import com.pravin.kafka.entity.Order;
import com.pravin.kafka.entity.OrderStatus;
import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.entity.Product;
import com.pravin.kafka.event.OrderCreatedEvent;
import com.pravin.kafka.event.OrderItemEvent;
import com.pravin.kafka.exception.ResourceNotFoundException;
import com.pravin.kafka.repository.OrderRepository;
import com.pravin.kafka.repository.OutboxRepository;
import com.pravin.kafka.repository.ProductRepository;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final DataMapper dataMapper;
    private final ProductRepository productRepository;
    private final ObjectMapper objectMapper;
    private UUID eventId;

    public OrderService(OrderRepository orderRepository,
                        ProductRepository productRepository,
                        DataMapper dataMapper,
                        OutboxRepository outboxRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.productRepository = productRepository;
        this.dataMapper = dataMapper;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(transactionManager = "transactionManager")
    public OrderResponse create(OrderRequest orderRequest, String correlationId) {
        // Currently, price is fetched from the product table, not from the UI.
        // In the future, discount logic may require taking the price from the UI.
        Order saved = saveAndGetOrder(orderRequest);
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                saved.getId(),
                saved.getItems().stream()
                        .map(i -> new OrderItemEvent(i.getProductId(), i.getQuantity()))
                        .toList()
        );
        createOutboxEvent(correlationId, saved, orderCreatedEvent);
        return dataMapper.toResponse(saved);
    }

    private void createOutboxEvent(String correlationId, Order saved, OrderCreatedEvent orderCreatedEvent) {
        UUID eventId = UUID.randomUUID();
        // publish event
        OutboxEvent event = new OutboxEvent();
        event.setId(eventId);
        event.setAggregateType("Order");
        event.setAggregateId(saved.getId());
        event.setCorrelationId(correlationId);
        event.setEventType("order.created");

        EventEnvelope<OrderCreatedEvent> envelope =
                new EventEnvelope<>(
                        eventId,
                        correlationId,
                        event.getEventType(),
                        event.getAggregateId(),
                        event.getAggregateType(),
                        LocalDateTime.now(),
                        orderCreatedEvent
                );

        try {
            event.setPayload(objectMapper.writeValueAsString(envelope));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize order event", e);
        }
        event.setStatus(OutboxEvent.Status.NEW);
        event.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(event);
        log.info(
                "Created outbox event {} for order {}",
                event.getId(),
                saved.getId()
        );
    }

    private @NonNull Order saveAndGetOrder(OrderRequest orderRequest) {
        BigDecimal totalAmount = orderRequest.items().stream()
                .map(item -> {
                    Product product = productRepository.findById(item.productId())
                            .orElseThrow(() -> new RuntimeException("Product not found"));

                    return product.getPrice()
                            .multiply(BigDecimal.valueOf(item.quantity()));
                })
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        Order order = dataMapper.toEntity(orderRequest);
        order.setStatus(OrderStatus.CREATED);
        order.setTotalAmount(totalAmount);
        order.getItems().forEach(item -> item.setOrder(order));
        Order saved = orderRepository.save(order);
        log.info("Order created.");
        return saved;
    }

    public OrderResponse get(Long id) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Order not found with id: " + id));
        return dataMapper.toResponse(order);
    }

    public List<OrderResponse> getByUser(Long userId) {
        return orderRepository.findByUserId(userId)
                .stream()
                .map(dataMapper::toResponse)
                .toList();
    }
}