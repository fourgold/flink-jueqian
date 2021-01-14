package com.ecust.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Jinxin Li
 * @create 2021-01-08 14:32
 * 传感器的样例类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long ts;
    private Double temp;
}
