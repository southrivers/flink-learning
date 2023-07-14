package com.hw.transform;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest_KeyBy {
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
        // 这里keyBy有大概4中类型：int变量适用于元组，string变量适用于javabean，还有keySelector适用于更一般的类型
        // 因为这里使用的是java bean的keyBy算子，所以默认是需要一个无参的构造函数
        // keyBy之后为什么是Tuple呢？是这里的keyBy里面传入的东西，这里之所以是Tuple是因为keyBy可以设置多个字段，如果传入一个代表了Tuple1
        // 通过tuple这种方式指定key更具有一般性
        // 参见：https://www.youtube.com/watch?v=EpzLCl03mNI&list=PLmOn9nNkQxJGgsR8xuYpSwkkx293BHtr5&index=29
        KeyedStream<SensorReading, Tuple> sensorIdStream = mapStream.keyBy("sensorId");
        // 这里的keyBy之后得到的结果是4、7一组，5、6一组
        sensorIdStream.print("keyBy");
        sensorIdStream.sum("temp").print("sum");
        env.execute("test");
    }
}
