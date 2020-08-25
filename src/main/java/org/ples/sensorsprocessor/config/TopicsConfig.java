package org.ples.sensorsprocessor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.ples.sensorsprocessor.stream.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicsConfig {

    @Bean
    public NewTopic sensorStateTopic() {
        return TopicBuilder.name(Topics.SENSOR_STATE).partitions(4).replicas(1).build();
    }

    @Bean
    public NewTopic personTopic() {
        return TopicBuilder.name(Topics.PERSON).partitions(4).replicas(1).build();
    }

    @Bean
    public NewTopic sensorTopic() {
        return TopicBuilder.name(Topics.SENSOR).partitions(4).replicas(1).build();
    }

    @Bean
    public NewTopic personSensorStateTopic() {
        return TopicBuilder.name(Topics.PERSON_SENSOR_STATE).partitions(4).replicas(1).build();
    }
}
