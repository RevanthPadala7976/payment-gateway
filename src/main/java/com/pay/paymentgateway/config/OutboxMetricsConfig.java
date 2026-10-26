package com.pay.paymentgateway.config;

import com.pay.paymentgateway.entity.OutboxStatus;
import com.pay.paymentgateway.repository.OutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OutboxMetricsConfig {

    private final MeterRegistry meterRegistry;
    private final OutboxRepository outboxRepository;

    public OutboxMetricsConfig(MeterRegistry meterRegistry, OutboxRepository outboxRepository) {
        this.meterRegistry = meterRegistry;
        this.outboxRepository = outboxRepository;
    }

    @PostConstruct
    public void registerGauges() {
        for (OutboxStatus status : OutboxStatus.values()) {
            Gauge.builder("outbox_events_count", () -> outboxRepository.countByStatus(status))
                    .tag("status", status.name())
                    .description("Number of outbox events currently in this status")
                    .register(meterRegistry);
        }
    }
}