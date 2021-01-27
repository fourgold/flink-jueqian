package com.ecust.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author JueQian
 * @create 01-20 9:51
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorAuto extends SensorReading{
    private String id;
    private Long ts;
    private Double temp1;
    private Double temp2;
}
