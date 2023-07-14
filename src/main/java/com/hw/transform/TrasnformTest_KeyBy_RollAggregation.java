package com.hw.transform;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TrasnformTest_KeyBy_RollAggregation {

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

        // 这里的tuple并不是返回的结果而是包装成用来选择流的key
        KeyedStream<SensorReading, Tuple> sensorIdStream = mapStream.keyBy("sensorId");

        // 这里经过sum、reduce等算子之后，就会由原来的KeyedStream转换成了普通的datastream了
        // 对于sum来说其他字段咋么选择呢，选择最小值
        sensorIdStream.sum("temp").print("sum");
        // 选择最先接收到的字段，这种情况多用在忽略其他字段或者说其他字段不重要的场景中
        sensorIdStream.max("temp").print("max");
        // 同步更新其他的字段信息
        sensorIdStream.maxBy("temp").print("maxBy");
        // 综上：
        env.execute("test");
    }
}
