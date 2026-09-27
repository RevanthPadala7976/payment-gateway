package com.pay.paymentgateway.service;

import com.pay.paymentgateway.dto.ProviderResponse;
import com.pay.paymentgateway.entity.Payment;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import com.pay.paymentgateway.provider.PaymentProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class PaymentRoutingService {
    private final PaymentProvider primaryProvider;
    private final PaymentProvider secondaryProvider;
    public PaymentRoutingService(
            @Qualifier("primaryProvider") PaymentProvider primaryProvider,
            @Qualifier("secondaryProvider") PaymentProvider secondaryProvider){
        this.primaryProvider = primaryProvider;
        this.secondaryProvider = secondaryProvider;
    }

    @CircuitBreaker(name = "primaryProvider", fallbackMethod = "fallbackSecondary")
    public ProviderResponse processWithCircuitBreaker(Payment payment){
        return primaryProvider.processPayment(payment);
    }

    public ProviderResponse fallbackSecondary(Payment payment, Throwable throwable) {
        System.out.println("Primary Bank degraded (" + throwable.getMessage() + "). Diverting traffic to Secondary Provider!");
        return secondaryProvider.processPayment(payment);
    }

    public ProviderResponse processBaseline(Payment payment) {
        return primaryProvider.processPayment(payment);
    }
}
