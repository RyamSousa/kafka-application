package org.example;

import java.math.BigDecimal;

public class Order {

    private final String orderId;
    private final String email;
    private final BigDecimal amount;

    public Order(String orderId, String email, BigDecimal amount) {
        this.orderId = orderId;
        this.amount = amount;
        this.email = email;
    }
}
