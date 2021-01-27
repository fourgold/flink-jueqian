package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading1 {
    private String id;
    private Long ts;
    private Double temp;
    private Integer count;
}
