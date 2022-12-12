package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException     {
        String url = "jdbc:sqlite:service-users/target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        this.connection.createStatement().execute("create table Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))");
    }

    public static void main(String[] args) throws SQLException {
        CreateUserService createUserService = new CreateUserService();
        try (var service = new KafkaService(
                CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("----------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.value());

        Order order = record.value();

        if (isNewUser(order.getEmail())){
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("insert into Users (uuid, email) values (?,?)");
        insert.setString(1, "uuid");
        insert.setString(2, "email");
        insert.execute();
        System.out.println("User inserted. uuid: uuidHere, email: "+ email);
    }

    private boolean isNewUser(String email) throws SQLException {
        var exists = connection.prepareStatement("select uuid from Users " +
                "where email = ? limit 1");

        exists.setString(1, email);
        var results = exists.executeQuery();
        return !results.next();
    }
}
