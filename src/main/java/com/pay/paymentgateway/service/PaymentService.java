package com.pay.paymentgateway.service;

import com.pay.paymentgateway.entity.Payment;
import com.pay.paymentgateway.entity.PaymentStatus;
import com.pay.paymentgateway.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository; // This talks to Database
    private final RedisTemplate<String, String> redisTemplate; // This talks to Redis to check for duplicate entries
    private final KafkaTemplate<String, String> kafkaTemplate; // This will be our messaging queue to do things

    private final ObjectMapper objectMapper; // To convert Java Objects to JSON

    @Autowired
    public PaymentService(PaymentRepository paymentRepository,
                          RedisTemplate<String, String> redisTemplate,
                          KafkaTemplate<String, String> kafkaTemplate,
                          ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }
    // Application logic

    public String initiatePayment(Payment payment) {
        // Add it to redis
        boolean isNew = redisTemplate.opsForValue().setIfAbsent(payment.getTransactionId(), PaymentStatus.PENDING.name());

        /**
         * Redis will return false, if the request already exist
         * Return true, if the request is new
         * If returns true, we save the permanent record to database
         * Give it to Kafka by returning PENDING message
         */
        if(isNew){
            payment.setStatus(PaymentStatus.PENDING);
            paymentRepository.save(payment);

            // Sending request to Kafka
            /**
             * The send Method takes 3 arguments:
             * 1. Topic name
             * 2. A Unique ID (KEY)
             * 3. Data to process
             *
             * This ensures, that all updates for a specific transaction (key) goes to the same kafka partition (Preserving their Order)
             */
            try {
                kafkaTemplate.send("payments", payment.getTransactionId(), objectMapper.writeValueAsString(payment));
                return "PENDING";
            } catch (Exception e) {
                // Release the Redis lock if Kafka publication fails
                redisTemplate.delete(payment.getTransactionId());
                throw new RuntimeException("Failed to enqueue payment: " + e.getMessage(), e);
            }
        }
        else{
            return redisTemplate.opsForValue().get(payment.getTransactionId());
        }
    }
}
