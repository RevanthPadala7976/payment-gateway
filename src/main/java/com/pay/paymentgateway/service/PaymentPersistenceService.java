package com.pay.paymentgateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pay.paymentgateway.entity.OutboxEvent;
import com.pay.paymentgateway.entity.OutboxStatus;
import com.pay.paymentgateway.entity.Payment;
import com.pay.paymentgateway.entity.PaymentStatus;
import com.pay.paymentgateway.repository.OutboxRepository;
import com.pay.paymentgateway.repository.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentPersistenceService {
    private final PaymentRepository paymentRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public PaymentPersistenceService(PaymentRepository paymentRepository,
                                     OutboxRepository outboxRepository,
                                     ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }
    @Transactional
    public void savePaymentAndOutbox(Payment payment) throws Exception{
        payment.setStatus(PaymentStatus.PENDING);
        paymentRepository.save(payment);

//        if (true) throw new RuntimeException("Simulated crash for testing"); // TEMP — remove after test
        String payloadJson = objectMapper.writeValueAsString(payment);
        OutboxEvent outboxEvent = new OutboxEvent(
                payment.getTransactionId(),
                payloadJson,
                "payments",
                OutboxStatus.PENDING
        );
        outboxRepository.save(outboxEvent);
    }
}
