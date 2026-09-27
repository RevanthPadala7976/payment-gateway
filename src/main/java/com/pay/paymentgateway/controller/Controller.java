package com.pay.paymentgateway.controller;

import com.pay.paymentgateway.entity.Payment;
import com.pay.paymentgateway.repository.PaymentRepository;
import com.pay.paymentgateway.service.PaymentService;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.pay.paymentgateway.dto.ProviderResponse;
import com.pay.paymentgateway.service.PaymentRoutingService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * TODO:
 * Test the Endpoints using Postman and Web
 * Create Refund endpoint, and few more if possible
 * Make sure this project has all the things mentioned in the Resume
 */
@RestController
@RequestMapping("/api/v1/payments")
public class Controller {
    private static final Logger log = LoggerFactory.getLogger(Controller.class);

    private final PaymentService paymentService;
    private final RedisTemplate<String, String> redisTemplate;
    private final PaymentRoutingService paymentRoutingService;
    private final PaymentRepository paymentRepository;

    public Controller(PaymentService paymentService,
                      RedisTemplate<String, String> redisTemplate,
                      PaymentRoutingService paymentRoutingService,
                      PaymentRepository paymentRepository) {
        this.paymentService = paymentService;
        this.redisTemplate = redisTemplate;
        this.paymentRoutingService = paymentRoutingService;
        this.paymentRepository = paymentRepository;
    }

    /**
     * ENDPOINT 1: Initiate payment (Async via Kafka)
     * URL: POST /api/v1/payments
     */
    @PostMapping
    public ResponseEntity<?> initiatePayment(@RequestBody Payment payment) {
        log.info("Received payment request for ID: {}", payment.getTransactionId());
        String status = paymentService.initiatePayment(payment);

        if ("PENDING".equals(status)) {
            log.info("Payment {} accepted and sent to Kafka.", payment.getTransactionId());
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(
                    Map.of(
                            "message", "Payment initiated successfully",
                            "transactionID", payment.getTransactionId(),
                            "status", status
                    )
            );
        } else {
            log.info("Payment {} was already processed. Returning cached status.", payment.getTransactionId());
            return ResponseEntity.ok(
                    Map.of(
                            "message", "Payment already exists",
                            "transactionID", payment.getTransactionId(),
                            "status", status
                    )
            );
        }
    }

    /**
     * ENDPOINT 2: Get status from Redis Cache (with DB fallback to keep in sync)
     * URL: GET /api/v1/payments/{transactionId}
     */
    @GetMapping("/{transactionId}")
    public ResponseEntity<?> getPaymentStatus(@PathVariable String transactionId) {
        log.info("Checking status for transaction: {}", transactionId);
        String cachedStatus = redisTemplate.opsForValue().get(transactionId);

        if (cachedStatus != null) {
            log.info("Found status {} in Redis for transaction: {}", cachedStatus, transactionId);
            return ResponseEntity.ok(Map.of("transactionID", transactionId, "status", cachedStatus));
        }

        // Cache fallback: Query database if Redis is empty or expired, then repopulate cache
        return paymentRepository.findByTransactionId(transactionId)
                .map(payment -> {
                    String dbStatus = payment.getStatus().name();
                    log.info("Found status {} in DB for transaction: {}. Updating Redis cache.", dbStatus, transactionId);
                    redisTemplate.opsForValue().set(transactionId, dbStatus);
                    return ResponseEntity.ok(Map.of("transactionID", transactionId, "status", dbStatus));
                })
                .orElseGet(() -> {
                    log.warn("Status not found in Redis or DB for transaction: {}", transactionId);
                    return ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(Map.of("message", "Transaction not found or still pending"));
                });
    }

    /**
     * Benchmark 1: The Fragile Baseline
     * URL: POST /api/v1/payments/benchmark/baseline
     */
    @PostMapping("/benchmark/baseline")
    public ResponseEntity<ProviderResponse> benchmarkBaseline(@RequestBody Payment payment) {
        try {
            ProviderResponse response = paymentRoutingService.processBaseline(payment);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Benchmark 2: The Resilient Path
     * URL: POST /api/v1/payments/benchmark/resilient
     */
    @PostMapping("/benchmark/resilient")
    public ResponseEntity<ProviderResponse> benchmarkResilient(@RequestBody Payment payment) {
        ProviderResponse response = paymentRoutingService.processWithCircuitBreaker(payment);
        return ResponseEntity.ok(response);
    }
}
