package com.hw.transform;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest_2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceStream = env.readTextFile("D:\\workspace\\github\\test-flink\\flink-test\\src\\main\\resources\\sensorreading.txt");
        sourceStream.print("source");
        DataStream<SensorReading> mapStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        mapStream.flatMap(new FlatMapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public void flatMap(SensorReading sensorReading, Collector<Tuple2<String, Double>> collector) throws Exception {
                collector.collect(new Tuple2<String, Double>(sensorReading.getSensorId(), sensorReading.getTemp()));
            }
        }).print("collector");
        env.execute("test");
    }
}
