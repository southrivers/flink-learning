package com.hw.source;

import com.hw.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest_2 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //
        DataStream<SensorReading> sourceStream = env.fromCollection(Arrays.asList(new SensorReading("sensor_1", 1547718199L, 35.8)));
        // 针对存在shuffle的操作来说，
        sourceStream.keyBy("sensorId").print("source-bean-constructor");

        env.execute("test");
    }
}
