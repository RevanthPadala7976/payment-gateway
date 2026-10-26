package com.pay.paymentgateway.service;

import com.pay.paymentgateway.entity.OutboxEvent;
import com.pay.paymentgateway.entity.OutboxStatus;
import com.pay.paymentgateway.entity.Payment;
import com.pay.paymentgateway.entity.PaymentStatus;
import com.pay.paymentgateway.repository.OutboxRepository;
import com.pay.paymentgateway.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Service
public class PaymentService {

    // NEW: carries both the status string AND whether this request was the
    // actual winner of the Redis lock — the controller needs both pieces.
    public record InitiationResult(boolean isNew, String status) {}

    private final RedisTemplate<String, String> redisTemplate;
    private final PaymentPersistenceService paymentPersistenceService;

    @Autowired
    public PaymentService(RedisTemplate<String, String> redisTemplate,
                          PaymentPersistenceService paymentPersistenceService) {
        this.redisTemplate = redisTemplate;
        this.paymentPersistenceService = paymentPersistenceService;
    }

    // CHANGED: return type is now InitiationResult, not String
    public InitiationResult initiatePayment(Payment payment) {
        boolean isNew = redisTemplate.opsForValue().setIfAbsent(
                payment.getTransactionId(),
                PaymentStatus.PENDING.name(),
                Duration.ofHours(24)
        );

        if (isNew) {
            try {
                paymentPersistenceService.savePaymentAndOutbox(payment);
                return new InitiationResult(true, "PENDING");
            } catch (Exception ex) {
                redisTemplate.delete(payment.getTransactionId());
                throw new RuntimeException("Failed to save payment and outbox event: " + ex.getMessage(), ex);
            }
        } else {
            String cachedStatus = redisTemplate.opsForValue().get(payment.getTransactionId());
            return new InitiationResult(false, cachedStatus);
        }
    }
}