package com.pravin.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/*
 http://localhost:8080/api-docs
 http://localhost:8080/swagger-ui/index.html
 http://localhost:8080/actuator/circuitbreakers
 http://localhost:8080/actuator/health

TO START KAFKA -   bin/kafka-server-start.sh config/server.properties
Stop Kafka with Ctrl + C
TO CHECK - bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

 Because of the limited resources I am creating all the service in one project
 Please create one microservice for each service
 1 - EmailNotificationService
 2 - OrderService
 3 - InventoryService
 4 - PaymentService
 5 - ShippingService
 6 - ProductService

*/



@SpringBootApplication
@EnableScheduling
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

}
