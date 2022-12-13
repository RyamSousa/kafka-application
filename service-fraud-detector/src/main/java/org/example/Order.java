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

    public BigDecimal getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", email='" + email + '\'' +
                ", amount=" + amount +
                '}';
    }

    public String getEmail() {
        return email;
    }
}
