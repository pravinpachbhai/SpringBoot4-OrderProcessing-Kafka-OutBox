package com.pravin.kafka.service;

import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.repository.OutboxRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Service
public class OutboxPublisherService {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherService.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxClaimService outboxClaimService;

    public OutboxPublisherService(
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  OutboxClaimService outboxClaimService) {
        this.kafkaTemplate = kafkaTemplate;
        this.outboxClaimService = outboxClaimService;
    }

    @Scheduled(fixedDelay = 500)
    public void publishEvents() {

        List<OutboxEvent> events  = outboxClaimService.claimBatch();
        if (events.isEmpty()) return;

        try {
                for (OutboxEvent event : events) {
                    publish(event.getId(), event.getEventType(), String.valueOf(event.getAggregateId()), event.getPayload());
                }
        } catch (Exception ex) {
            log.error("Kafka transaction failed for batch", ex);
        }
    }

    public void publish(UUID eventId, String topic, String key, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex == null) {
                outboxClaimService.markSuccess(eventId);
            } else {
                outboxClaimService.handleFailure(eventId, ex);
            }
        });
    }



}
