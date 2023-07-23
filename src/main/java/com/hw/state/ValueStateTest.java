package com.hw.state;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ValueStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        mapSource.keyBy("sensorId").map(new RichMapFunction<SensorReading, Integer>() {
            // 在这里声明state，之所以不在这里初始化state是因为这里的初始化是在jobmanager里面实现的初始化
            // 不过getRuntimeContext肯定是在taskmanager里面才可以实现调用的，因此这里只做声明，不做定义
            ValueState<Integer> count;

            @Override
            public void open(Configuration parameters) throws Exception {
                // State的定义伴随着一个statedescriptor
                count = getRuntimeContext().getState(new ValueStateDescriptor<>("key-count", Integer.class));
            }

            @Override
            public Integer map(SensorReading sensorReading) throws Exception {
                Integer value = count.value();
                // 可以在定义的时候声明初始值，也可以在获取的时候声明初始值
                if (value == null) {
                    value = 0;
                }
                value++;
                count.update(value);
                return value;
            }
        }).print("state-count");

        env.execute();
    }
}
