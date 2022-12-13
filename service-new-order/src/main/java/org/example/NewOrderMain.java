package org.example;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailDispatcher = new KafkaDispatcher<String>()) {
                var email = Math.random() + "@gmail.com";
                for (int i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                    Order order = new Order(orderId, email, amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                    var emailCode = "Thanks for your order! We are processing your order";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
                }
            }
        }
    }


}
