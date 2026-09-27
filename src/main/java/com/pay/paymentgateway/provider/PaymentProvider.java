package com.pay.paymentgateway.provider;

import com.pay.paymentgateway.dto.ProviderResponse;
import com.pay.paymentgateway.entity.Payment;

public interface PaymentProvider {
    ProviderResponse processPayment(Payment payment);
    String getProviderName();
}
