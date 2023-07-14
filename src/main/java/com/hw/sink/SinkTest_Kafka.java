package com.hw.sink;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class SinkTest_Kafka {

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

        Properties props = new Properties();
        // 注意这里的produce里面的泛型是因为上游也是这种类型的数据，并不是要写入到kafka里面的类型是SensorReading，具体
        // 写入到kafka里面什么数据是和其序列化器是相关的
        mapStream.addSink(new FlinkKafkaProducer011<SensorReading>("192.168.28.3:9092", "sensor", new SerializationSchema<SensorReading>() {
            // 这里是写入到kafka里面的数据，上面的泛型是上游的类型，并不是写入到下游的类型
            @Override
            public byte[] serialize(SensorReading sensorReading) {
                return sensorReading.toString().getBytes();
            }
        }));
        env.execute();
    }
}
