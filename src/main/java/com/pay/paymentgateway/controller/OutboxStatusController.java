package com.pay.paymentgateway.controller;

import com.pay.paymentgateway.entity.OutboxStatus;
import com.pay.paymentgateway.repository.OutboxRepository;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/outbox")
public class OutboxStatusController {

    private final OutboxRepository outboxRepository;

    public OutboxStatusController(OutboxRepository outboxRepository) {
        this.outboxRepository = outboxRepository;
    }

    @GetMapping("/summary")
    public Map<String, Long> getSummary() {
        Map<String, Long> summary = new LinkedHashMap<>();
        for (OutboxStatus status : OutboxStatus.values()) {
            summary.put(status.name(), outboxRepository.countByStatus(status));
        }
        return summary;
    }
}