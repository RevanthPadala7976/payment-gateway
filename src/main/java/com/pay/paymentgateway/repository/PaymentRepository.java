package com.pay.paymentgateway.repository;

import com.pay.paymentgateway.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {
    // We need this to check Idempotency (Checking if order already exists)
    Optional<Payment> findByTransactionId(String id);
}
