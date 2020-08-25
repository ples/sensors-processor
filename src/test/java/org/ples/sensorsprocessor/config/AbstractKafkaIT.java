package org.ples.sensorsprocessor.config;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;


public abstract class AbstractKafkaIT {

    private static final KafkaContainer kafkaContainer = new KafkaContainer("5.4.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        kafkaContainer.start();
        registry.add("spring.kafka.properties.bootstrap.servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.consumer.properties.auto.offset.reset", () -> "earliest");
    }
}
