package com.pravin.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pravin.kafka.dto.EventEnvelope;
import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class OutboxPublisherService {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherService.class);
    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxPublisherService(OutboxRepository repository,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional("transactionManager")
    @Scheduled(fixedDelay = 500)
    public void publishEvents() {

        List<OutboxEvent> events = repository.findTop100ByStatusOrderByCreatedAtAsc(OutboxEvent.Status.NEW);
        if (events.isEmpty()) return;
        try {
            kafkaTemplate.executeInTransaction(ops -> {
                for (OutboxEvent event : events) {
                    boolean success = true;
                    EventEnvelope envelope = new EventEnvelope(event.getId(),
                            event.getEventType(),
                            event.getAggregateId(),
                            event.getAggregateType(), LocalDateTime.now(), event.getPayload());
                    try {
                        ops.send(event.getEventType(), String.valueOf(event.getAggregateId()), objectMapper.writeValueAsString(envelope));
                    } catch (Exception e) {
                        success = false;
                        event.setStatus(OutboxEvent.Status.FAILED);
                    }
                    event.setUpdatedAt(LocalDateTime.now());
                    if (success) {
                        event.setStatus(OutboxEvent.Status.PUBLISHED);
                    }
                }
                return true;
            });

        } catch (Exception ex) {
            log.error("Kafka transaction failed for batch", ex);
        }
    }
}
