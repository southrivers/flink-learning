package com.hw.transform;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;
import java.util.Collections;

public class TransformTest_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceStream = env.readTextFile("D:\\workspace\\github\\test-flink\\flink-test\\src\\main\\resources\\sensorreading.txt");
        DataStream<SensorReading> mapStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        // 注意这里的split操作其实相当于给原始的datastream打上一个tag，当然这里的tag可以是多个，类似于keyBy操作，后面必须跟上特定的算子
        // 才可以再次转换成Datastream，因此这里的split操作并不是真正意义上变成了两条流。
        SplitStream<SensorReading> splitStream = mapStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                if (value.getSensorId().equals("sensor_1")) {
                    return Arrays.asList("red", "blue");
                } else {
                    return Collections.singleton("black");
                }
            }
        });

        DataStream<SensorReading> black = splitStream.select("black");
        DataStream<SensorReading> red = splitStream.select("red");
        // connect之后，两条流看起来合并成一条流了，不过真实的情况是并没有立即合成一条流，原因是两条流的类型可能不同
        // 因此需要分别调用两条流的map函数，将两条流转换成一种类型的流
        DataStream<SensorReading> connectStream = black.connect(red).map(new CoMapFunction<SensorReading, SensorReading, SensorReading>() {
            @Override
            public SensorReading map1(SensorReading value) throws Exception {
                return value;
            }

            @Override
            public SensorReading map2(SensorReading value) throws Exception {
                return value;
            }
        });
        connectStream.print("connect-comap");
        env.execute();
    }
}
