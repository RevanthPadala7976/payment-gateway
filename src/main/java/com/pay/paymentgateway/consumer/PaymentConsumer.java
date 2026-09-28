package com.pay.paymentgateway.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pay.paymentgateway.dto.ProviderResponse;
import com.pay.paymentgateway.entity.Payment;
import com.pay.paymentgateway.entity.PaymentStatus;
import com.pay.paymentgateway.repository.PaymentRepository;
import com.pay.paymentgateway.service.PaymentRoutingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentConsumer {
    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private final PaymentRepository paymentRepository;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final PaymentRoutingService paymentRoutingService;

    public PaymentConsumer(PaymentRepository paymentRepository,
                           RedisTemplate<String, String> redisTemplate,
                           ObjectMapper objectMapper,
                           PaymentRoutingService paymentRoutingService) {
        this.paymentRepository = paymentRepository;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.paymentRoutingService = paymentRoutingService;
    }

    /**
     * Triggered automatically by Spring Kafka whenever a new message lands on the "payments" topic
     * No one calls this directly. This message can arrive from two places:
     * 1. PaymentService (old direct path, now unused [without scheduling])
     * 2. OutboxPoller (current path, reads PENDING rows from outbox_event and publish them here)
     * This method does not care which one sent it.
     * *
     * Flow:
     * deserialized JSON -> Payment, run it through the circuit breaker
     * (PaymentRoutingService), then persist the final APPROVAL/DECLINED STATUS to Postgres and Redis
     */
    @KafkaListener(topics = "payments", groupId = "payment-group")
    public void receivePayment(String message) {
        try {
            log.info("[KAFKA] Received raw message: {}", message);

            Payment payment = objectMapper.readValue(message, Payment.class);
            log.info("[ROUTING] Processing payment for Transaction: {}", payment.getTransactionId());

            // 2. Call the Circuit-Breaker protected routing service
            ProviderResponse response = paymentRoutingService.processWithCircuitBreaker(payment);

            PaymentStatus finalStatus = response.success() ? PaymentStatus.APPROVED : PaymentStatus.DECLINED;

            // 3. Update Database
            Payment dbPayment = paymentRepository.findByTransactionId(payment.getTransactionId())
                    .orElse(payment);
            dbPayment.setStatus(finalStatus);
            paymentRepository.save(dbPayment);
            log.info("[DB] Transaction {} completed via {} with status: {}",
                    dbPayment.getTransactionId(), response.providerName(), finalStatus);

            // 4. Update Redis Cache
            redisTemplate.opsForValue().set(dbPayment.getTransactionId(), finalStatus.name());
            log.info("[REDIS] Cache updated for Transaction: {} with status: {}", dbPayment.getTransactionId(), finalStatus);
            log.info("--------------------------------------------------");

        } catch (Exception e) {
            log.error("[ERROR] Failed to process payment message: {}", e.getMessage(), e);
        }
    }
}
