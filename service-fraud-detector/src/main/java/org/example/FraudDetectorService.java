package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    private static final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
        try (var service = new KafkaService(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                FraudDetectorService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }
    }

    private static void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // Ignoring
            e.printStackTrace();
        }
        Order order = record.value();
        if(isFraud(order)){
            System.out.println("Order is fraud!!!! "+order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        }

        System.out.println("Order approved: "+ order);
        orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
