package org.ples.sensorsprocessor.stream.dto;

import lombok.*;

import java.util.UUID;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TemperatureSensorState {

    public final static float HIGH = 37.0f;

    private UUID sensorUuid;
    private float value;
}
