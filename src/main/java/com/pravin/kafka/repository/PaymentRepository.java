package com.pravin.kafka.repository;

import com.pravin.kafka.entity.Payment;
import org.jspecify.annotations.Nullable;
import org.junit.platform.commons.function.Try;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByOrderId(Long orderId);
}