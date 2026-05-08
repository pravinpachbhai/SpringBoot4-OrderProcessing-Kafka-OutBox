package com.pravin.kafka.service;

import com.pravin.kafka.entity.OutboxEvent;
import com.pravin.kafka.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Service
public class OutboxClaimService {
    private final Random random = new Random();
    private final OutboxRepository repository;
    private static final Logger log = LoggerFactory.getLogger(OutboxClaimService.class);
    public OutboxClaimService(OutboxRepository repository) {
        this.repository = repository;
    }

    @Transactional(transactionManager = "transactionManager")
    public List<OutboxEvent> claimBatch() {

        //List<OutboxEvent> events = repository.findTop100ByStatusOrderByCreatedAtAsc(OutboxEvent.Status.NEW);
        List<OutboxEvent> events =
                repository.findEventsToPublish(
                        List.of(
                                OutboxEvent.Status.NEW,
                                OutboxEvent.Status.RETRY
                        ),
                        PageRequest.of(0, 100)
                );


        if (events.isEmpty()) return List.of();

        List<UUID> ids = events.stream().map(OutboxEvent::getId).toList();
        int count = repository.processingEvents(ids);

        log.info("Updated outbox for processing record ={}", count);

        return events;
    }

    @Transactional(transactionManager = "transactionManager")
    public void markSuccess(UUID eventId) {
        repository.findById(eventId).ifPresent(event -> {
            event.setStatus(OutboxEvent.Status.PUBLISHED);
            event.setProcessedAt(LocalDateTime.now());
            event.setUpdatedAt(LocalDateTime.now());
            repository.save(event);
        });
    }

    @Transactional(transactionManager = "transactionManager")
    public void handleFailure(UUID eventId, Throwable e) {
        repository.findById(eventId).ifPresent(event -> {
            int retries = event.getRetryCount() + 1;
            event.setRetryCount(retries);
            event.setErrorMessage(e.getMessage());
            event.setUpdatedAt(LocalDateTime.now());
            if (retries >= 5) {
                event.setStatus(OutboxEvent.Status.DEAD);
            } else {
                event.setStatus(OutboxEvent.Status.RETRY);
                long delay = Math.min((1L << retries) + random.nextInt(3), 60);
                event.setNextRetryAt(LocalDateTime.now().plusSeconds(delay));
            }
            repository.save(event);
        });
    }
}
