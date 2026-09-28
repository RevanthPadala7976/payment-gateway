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

@Service
public class PaymentService {
    private final RedisTemplate<String, String> redisTemplate; // This talks to Redis to check for duplicate entries
    private final PaymentPersistenceService paymentPersistenceService;

    @Autowired
    public PaymentService(RedisTemplate<String, String> redisTemplate,
                          PaymentPersistenceService paymentPersistenceService) {
        this.redisTemplate = redisTemplate;
        this.paymentPersistenceService = paymentPersistenceService;
    }
    // Application logic

    public String initiatePayment(Payment payment) {
        // Add it to redis
        boolean isNew = redisTemplate.opsForValue().setIfAbsent(payment.getTransactionId(), PaymentStatus.PENDING.name());

        /* *
         * Redis will return false, if the request already exist
         * Return true, if the request is new
         * If returns true, we save the permanent record to database
         * Give it to Kafka by returning PENDING message
         * */
        if(isNew) {
            try {
                paymentPersistenceService.savePaymentAndOutbox(payment);
                return "PENDING";
            } catch (Exception ex) {
                redisTemplate.delete(payment.getTransactionId());
                throw new RuntimeException("Failed to save payment and outbox event: "+ ex.getMessage(), ex);
            }
        } else{
            return redisTemplate.opsForValue().get(payment.getTransactionId());
        }
    }
}
