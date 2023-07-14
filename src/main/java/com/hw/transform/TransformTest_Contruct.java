package com.hw.transform;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest_Contruct {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceStream = env.socketTextStream("192.168.28.3", 7777);
        DataStream<SensorReading> mapStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        // RichFunction是抽象类，不是接口
        MyConstructMap myConstructMap = new MyConstructMap("test-property");
        mapStream.map(myConstructMap).print("rich-function");
        env.execute();
    }

    public static class MyConstructMap extends RichMapFunction<SensorReading, SensorReading> {

        // 这里使用构造函数创建的属性最终sout只会执行一次，可见其是在jobmanager上执行的，因此生产环境使用open方法是比较好的方案
        private String test;

        public MyConstructMap(String test) {
            this.test = test;
            System.out.println(test);
        }

        @Override
        public SensorReading map(SensorReading sensorReading) throws Exception {
            return sensorReading;
        }
    }
}
