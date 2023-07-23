package com.hw.state;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 基于状态实现温度跳变的检测
 */
public class TempJumpStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 状态存放在taskmanager上面，checkpoint数据存放在jobmanager的内存上
        env.setStateBackend(new MemoryStateBackend());
        // 状态存放在taskmanager上面，checkpoint数据存放在hdfs上
        env.setStateBackend(new FsStateBackend(""));
        // 状态存在rocksdb里面，checkpoint数据存放在hdfs上
        env.setStateBackend(new RocksDBStateBackend(""));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.5:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hello");

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), props);

        DataStreamSource<String> dataStreamSource = env.addSource(consumer011);

        // 解析数据
        SingleOutputStreamOperator<SensorReading> mapSource = dataStreamSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        mapSource.keyBy("sensorId").flatMap(new RichFlatMapFunction<SensorReading, String>() {
            ValueState<Double> lastTemp;

            @Override
            public void open(Configuration parameters) throws Exception {
                // State的定义伴随着一个statedescriptor
                lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("key-count", Double.class));
            }
            @Override
            public void flatMap(SensorReading sensorReading, Collector<String> collector) throws Exception {
                Double last = lastTemp.value();
                if (last != null) {
                    double v = Math.abs(sensorReading.getTemp() - last);
                    if (v > 10.0) {
                        collector.collect(sensorReading.toString());
                    }
                }
                lastTemp.update(sensorReading.getTemp());
            }
        }).print("state-count");

        env.execute();
    }
}
