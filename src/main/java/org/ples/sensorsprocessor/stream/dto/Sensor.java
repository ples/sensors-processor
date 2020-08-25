package org.ples.sensorsprocessor.stream.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Sensor {
    private UUID uuid;
    private UUID ownerUuid;
    private String name;
}
