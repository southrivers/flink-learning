package com.hw.transform;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Collections;

public class TransformTest_Split {

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
        // 这里如果给同一个流打上不同的标签，然后这里进行select这几个标签的话，虽然看着应该是重复选择多条数据，但是最终结果只有一份
        // 并不会随着标签的增多而出现多个元素
        splitStream.select("red", "black").print("red&black");
        splitStream.select("red", "blue").print("red&blue");
        splitStream.select("black").print("black");
        splitStream.select("red").print("red");
        env.execute();
    }
}
