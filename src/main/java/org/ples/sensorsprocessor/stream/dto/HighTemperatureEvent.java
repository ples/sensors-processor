package org.ples.sensorsprocessor.stream.dto;

import lombok.*;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class HighTemperatureEvent {
    private Person person;
    private Sensor sensor;
    private TemperatureSensorState sensorState;
}
