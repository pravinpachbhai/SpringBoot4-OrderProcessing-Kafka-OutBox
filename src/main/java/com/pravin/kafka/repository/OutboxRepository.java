package com.pravin.kafka.repository;

import com.pravin.kafka.entity.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {
    List<OutboxEvent> findTop100ByStatusOrderByCreatedAtAsc(OutboxEvent.Status status);

    //AND (e.nextRetryAt IS NULL OR e.nextRetryAt <= CURRENT_TIMESTAMP)
    @Query("""
            SELECT e
            FROM OutboxEvent e
            WHERE e.status IN (:statuses)
            AND (e.nextRetryAt IS NULL OR e.retryCount < 5)
            ORDER BY e.createdAt ASC
            """)
    List<OutboxEvent> findEventsToPublish(
            @Param("statuses") List<OutboxEvent.Status> statuses,
            Pageable pageable
    );

    @Modifying
    @Query("""
            UPDATE OutboxEvent e
            SET e.status = 'PROCESSING'
            WHERE e.id IN (:ids)
            AND e.status IN ('NEW','RETRY')
            """)
    int processingEvents(List<UUID> ids);


}
