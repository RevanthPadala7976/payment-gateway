package com.pay.paymentgateway.service;

import com.pay.paymentgateway.entity.OutboxEvent;
import com.pay.paymentgateway.entity.OutboxStatus;
import com.pay.paymentgateway.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OutboxPoller {
    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxRepository outboxRepository;

    private static final int MAX_RETRY_COUNT = 3;

    public OutboxPoller(KafkaTemplate<String, String> kafkaTemplate,  OutboxRepository outboxRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.outboxRepository = outboxRepository;
    }

    @Scheduled(fixedRate = 6000)
    public void outboxScheduler(){
        int claimed = outboxRepository.claimPendingEvents();
        if(claimed == 0){
            return;
        }
        log.info("[OUTBOX] Claimed {} event(s) for processing", claimed);
        List<OutboxEvent> claimedEvents = outboxRepository.findAllByStatus(OutboxStatus.PROCESSING);

        for(OutboxEvent event : claimedEvents){
            try{
                kafkaTemplate.send(event.getTopic(), event.getTransactionId(), event.getPayload());
                event.setStatus(OutboxStatus.SENT);
                outboxRepository.save(event);
            } catch (Exception e){
                log.error("[OUTBOX] failed to Published {} : {}", event.getTransactionId(), e.getMessage());
                event.incrementRetryCount();
                if(event.getRetryCount() >= MAX_RETRY_COUNT){
                    event.setStatus(OutboxStatus.FAILED);
                }
                outboxRepository.save(event);
            }
        }
    }
}
