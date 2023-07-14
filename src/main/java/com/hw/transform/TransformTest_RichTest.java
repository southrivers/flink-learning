package com.hw.transform;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest_RichTest {

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
        mapStream.map(new RichMapFunction<SensorReading, SensorReading>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                System.out.println("close");
            }

            @Override
            public SensorReading map(SensorReading sensorReading) throws Exception {
                return sensorReading;
            }
        }).print("rich-function");

        env.execute();
    }
}
