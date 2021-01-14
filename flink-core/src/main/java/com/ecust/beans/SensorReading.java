package com.ecust.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author Jinxin Li
 * @create 2021-01-08 14:32
 * 传感器的样例类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading implements Comparable{
    private String id;
    private Long ts;
    private Double temp;

    @Override
    public int compareTo(Object o) {
        SensorReading next = (SensorReading) o;
        return (int) (this.temp- next.temp)*10;
    }
}
