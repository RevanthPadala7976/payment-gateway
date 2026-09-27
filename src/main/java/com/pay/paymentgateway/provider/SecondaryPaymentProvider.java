package com.pay.paymentgateway.provider;

import com.pay.paymentgateway.dto.ProviderResponse;
import com.pay.paymentgateway.entity.Payment;
import org.springframework.stereotype.Component;

@Component("secondaryProvider")
public class SecondaryPaymentProvider implements PaymentProvider {

    @Override
    public ProviderResponse processPayment(Payment payment){
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return new ProviderResponse(true,
                "SECONDARY_PROVIDER",
                payment.getTransactionId(),
                "Payment authorized by Secondary Provider");
    }

    @Override
    public String getProviderName(){
        return "SECONDARY_PROVIDER";
    }
}
