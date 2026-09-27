package com.pay.paymentgateway.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true) // "PaymentID"
    private String transactionId;

    private String userEmail;

    @Column(nullable = false)
    private BigDecimal amount;

    private String currency;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;

    private LocalDateTime createdAt = LocalDateTime.now();

    // Constructor

    /**
     * JPA needs no-argument constructor
     * So, it can create an instance of the class using reflection before it starts mapping the database columns to files
     */
    public Payment() {}

    // All argument constructor - For application Logic
    public Payment(String transactionId, String userEmail, BigDecimal amount, String currency, PaymentStatus status) {
        this.transactionId = transactionId;
        this.userEmail = userEmail;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.createdAt = LocalDateTime.now();
    }

    // Getters
    public Long getId() {
        return id;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    // Setter for Status --> We need to update the status once Kafka finished processing
    public void setStatus(PaymentStatus status) {
        this.status = status;
    }
}