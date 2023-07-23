package com.hw.state;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 配置了重启策略之后有一个最大的重试次数
 */
public class RestartStrategyTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 固定延迟重启：对应参数，尝试重启的次数，两次重启的间隔，这里的参数的含义代表最多尝试重启3次，重启不成功那么重启之间的时间间隔是10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.5:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hello");

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), props);

        DataStreamSource<String> dataStreamSource = env.addSource(consumer011);

        // 解析数据
        SingleOutputStreamOperator<Integer> mapSource = dataStreamSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {

                return Integer.parseInt(s);
            }
        });

        mapSource.print("restart-test");

        env.execute();
    }
}
