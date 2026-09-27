package com.pay.paymentgateway.dto;

public record ProviderResponse(
        boolean success,
        String providerName,
        String transactionId,
        String message
) {}
