package org.ples.sensorsprocessor.stream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.ples.sensorsprocessor.stream.dto.HighTemperatureEvent;
import org.ples.sensorsprocessor.stream.dto.Person;
import org.ples.sensorsprocessor.stream.dto.Sensor;
import org.ples.sensorsprocessor.stream.dto.TemperatureSensorState;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.UUID;

/**
 *  Main application stream topology configuration. Contains all topology streams and tables.
 */
@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    public KTable<UUID, Person> personsTable(StreamsBuilder kStreamsBuilder) {
        return kStreamsBuilder.table(Topics.PERSON,
                Consumed.with(Serdes.UUID(), new JsonSerde<>(Person.class)));
    }

    public KTable<UUID, Sensor> sensorsTable(StreamsBuilder kStreamsBuilder) {
        return kStreamsBuilder.table(Topics.SENSOR,
                Consumed.with(Serdes.UUID(), new JsonSerde<>(Sensor.class)));
    }

    public KStream<UUID, TemperatureSensorState> sensorStateStream(StreamsBuilder kStreamsBuilder) {
        return kStreamsBuilder.stream(Topics.SENSOR_STATE, Consumed.with(Serdes.UUID(),
                new JsonSerde<>(TemperatureSensorState.class)));
    }

    @Bean
    public Topology highTemperatureIncidentTopology(StreamsBuilder kStreamsBuilder) {
        KTable<UUID, Sensor> sensorTable = sensorsTable(kStreamsBuilder);
        KStream<UUID, TemperatureSensorState> temperatureSensorStateStream = sensorStateStream(kStreamsBuilder);
        KTable<UUID, Person> personTable = personsTable(kStreamsBuilder);
        temperatureSensorStateStream
                .filter((key, value) -> value.getValue() > TemperatureSensorState.HIGH)
                .join(sensorTable, (sensorState, sensor) -> new HighTemperatureEvent()
                        .withSensorState(sensorState)
                        .withSensor(sensor))
                // Changing the key to match required persons partitions
                .selectKey((key, value) -> value.getSensor().getOwnerUuid())
                // Implicit repartitioning applied to set serde manually
                .through(Topics.PERSON_SENSOR_STATE,
                        Produced.with(Serdes.UUID(), temperatureEventSerde()))
                .leftJoin(personTable, (event, person) -> {
                    event.setPerson(person);
                    return event;
                })
                .to(Topics.EVENTS, Produced.with(Serdes.UUID(), temperatureEventSerde()));
        return kStreamsBuilder.build();
    }

    @Bean
    public Serde<HighTemperatureEvent> temperatureEventSerde() {
        return new JsonSerde<>(HighTemperatureEvent.class);
    }

}
