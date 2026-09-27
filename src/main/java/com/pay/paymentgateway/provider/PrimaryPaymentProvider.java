package com.pay.paymentgateway.provider;

import com.pay.paymentgateway.dto.ProviderResponse;
import com.pay.paymentgateway.entity.Payment;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component("primaryProvider")
public class PrimaryPaymentProvider implements PaymentProvider {
    @Override
    public ProviderResponse processPayment(Payment payment) {

        int chance = ThreadLocalRandom.current().nextInt(0, 100);

        if(chance < 15){
            // Simulating Failure: hanging connection followed by failure
            try{
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Primary bank timeout / 500 error");
        }

        // Simulation: realistic network round-trip depay
        try{
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return new ProviderResponse(true,
                "PRIMARY_PROVIDER",
                payment.getTransactionId(),
                "Payment authorized by Primary Provider");
    }

    @Override
    public String getProviderName() {
        return "PRIMARY_PROVIDER";
    }
}
