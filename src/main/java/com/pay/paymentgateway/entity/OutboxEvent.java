package com.pay.paymentgateway.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "outboxEvent", indexes = {@Index(name="idx_outbox_status", columnList = "status")})
public class OutboxEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long outboxId;

    @Column(nullable = false, unique = true)
    private String transactionId;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    @Column(nullable = false)
    private int retryCount = 0;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    public OutboxEvent() {}
    public OutboxEvent(String transactionId, String payload, String topic, OutboxStatus status) {
        this.transactionId = transactionId;
        this.payload = payload;
        this.topic = topic;
        this.status = status;
        this.createdAt = LocalDateTime.now();
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }
    public Long getOutboxId() { return outboxId; }
    public String getTransactionId() { return transactionId; }
    public String getPayload() { return payload; }
    public String getTopic() { return topic; }
    public OutboxStatus getStatus() { return status; }
    public int getRetryCount() { return retryCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }

    public void setStatus(OutboxStatus status) {
        this.status = status;
    }
}
