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
public class Person {
    private UUID uuid;
    private String name;
}
