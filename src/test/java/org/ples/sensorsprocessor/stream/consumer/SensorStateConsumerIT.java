package org.ples.sensorsprocessor.stream.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.ples.sensorsprocessor.config.AbstractKafkaIT;
import org.ples.sensorsprocessor.stream.Topics;
import org.ples.sensorsprocessor.stream.dto.HighTemperatureEvent;
import org.ples.sensorsprocessor.stream.dto.Person;
import org.ples.sensorsprocessor.stream.dto.Sensor;
import org.ples.sensorsprocessor.stream.dto.TemperatureSensorState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class SensorStateConsumerIT extends AbstractKafkaIT {

    @Autowired
    private KafkaTemplate<UUID, Object> template;
    @Autowired
    private ConsumerFactory<UUID, HighTemperatureEvent> consumerFactory;

    private Consumer<UUID, HighTemperatureEvent> consumer;

    @BeforeEach
    public void beforeEach() {
        consumer = consumerFactory.createConsumer("test-group", "test");
        consumer.subscribe(Collections.singletonList(Topics.EVENTS));
    }

    @Test
    public void shouldReceiveHighTemperatureEvent() {
        // Given
        float highTemperature = 38.0f;
        UUID sensorUuid = UUID.randomUUID();
        UUID personUuid = UUID.randomUUID();
        TemperatureSensorState state = new TemperatureSensorState()
                .withSensorUuid(sensorUuid)
                .withValue(highTemperature);
        Person person = new Person(personUuid, "John Doe");
        Sensor sensor = new Sensor(sensorUuid, personUuid, "Some termometer");
        // When
        template.send(Topics.PERSON, personUuid, person);
        template.send(Topics.SENSOR, sensorUuid, sensor);
        template.send(Topics.SENSOR_STATE, sensorUuid, state);
        // Then
        ConsumerRecords<UUID, HighTemperatureEvent> records = KafkaTestUtils.getRecords(consumer, 5000);
        assertThat(records).isNotEmpty();
        assertThat(records).hasSize(1);
        HighTemperatureEvent value = records.iterator().next().value();
        TemperatureSensorState sensorState = value.getSensorState();
        assertThat(sensorState).isNotNull();
        assertThat(sensorState.getSensorUuid()).isEqualTo(sensorUuid);
        assertThat(sensorState.getValue()).isEqualTo(highTemperature);
        assertThat(value.getPerson()).isNotNull();
        assertThat(value.getPerson().getName()).isNotEmpty();
        assertThat(value.getPerson().getName()).isEqualTo(person.getName());
        assertThat(value.getPerson().getUuid()).isEqualTo(personUuid);
        assertThat(value.getSensor()).isNotNull();
        assertThat(value.getSensor().getName()).isNotEmpty();
        assertThat(value.getSensor().getName()).isEqualTo(sensor.getName());
    }

    @Test
    public void shouldIgnoreNormalTemperatureValue() {
        // Given
        float temperature = 36.6f;
        UUID sensorUuid = UUID.randomUUID();
        UUID personUuid = UUID.randomUUID();
        TemperatureSensorState state = new TemperatureSensorState()
                .withSensorUuid(sensorUuid)
                .withValue(temperature);
        Person person = new Person(personUuid, "John Doe");
        Sensor sensor = new Sensor(sensorUuid, personUuid, "Some termometer");
        // When
        template.send(Topics.PERSON, personUuid, person);
        template.send(Topics.SENSOR, sensorUuid, sensor);
        template.send(Topics.SENSOR_STATE, sensorUuid, state);
        // Then
        ConsumerRecords<UUID, HighTemperatureEvent> records = KafkaTestUtils.getRecords(consumer, 5000);
        assertThat(records).isEmpty();
    }

}