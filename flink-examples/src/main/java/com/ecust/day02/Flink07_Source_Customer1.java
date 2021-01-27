package day02;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Flink07_Source_Customer1 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义数据源读取数据
        DataStreamSource<SensorReading> source = env.addSource(new MySource1());

        //3.打印
        source.print();

        //4.启动
        env.execute();

    }


    private static class MySource1 implements SourceFunction<SensorReading>{

        //创建一个随机数
        private Random random = new Random();

        //创建一个集合存放传感器对象
        private Map<String,SensorReading> maps = new HashMap<String,SensorReading>();

        //创建一个采集停止的标志
        private Boolean flag = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            //给传感器赋基准值
            for (int i = 0; i < 10; i++) {
                String id = "sensor_" + ( i + 1 );
                SensorReading sensorReading = new SensorReading(id, System.currentTimeMillis(), 60D + random.nextGaussian() * 20);
                maps.put(id,sensorReading);
            }

            //改变传感器对象温度
            while (flag){
                Set<String> ids = maps.keySet();
                for (String id : ids) {
                    SensorReading sensor = maps.get(id);
                    double v = sensor.getTemp() + random.nextGaussian();
                    sensor.setTemp(v);
                    ctx.collect(sensor);
                }
                Thread.sleep(5000);
            }

        }

        @Override
        public void cancel() {
            flag=false;

        }
    }
}
