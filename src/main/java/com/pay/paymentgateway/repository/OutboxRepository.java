package com.pay.paymentgateway.repository;

import com.pay.paymentgateway.entity.OutboxEvent;
import com.pay.paymentgateway.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {
    List<OutboxEvent> findAllByStatus(OutboxStatus status);

    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent e SET e.status = 'PROCESSING' WHERE e.status = 'PENDING'")
    int claimPendingEvents();

}