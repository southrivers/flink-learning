package com.hw.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * flink 消费kafka默认是异步提交，由于consumer是批量拉取数据，而commit的时候是提交这一批数据里面最新的数据，
 * 由于这一批数据在已经提交offset的情况下可能存在还没有消费完的情况，如果此时任务发生了重启，那么就会丢失一部分数据（还没有来得及消费的这一部分）
 * 还存在一种情况，因为是异步提交的，因此有可能已经消费完数据了，但是还没有来得及提交offset，那么此时任务挂掉的话，就会存在重复消费的问题。
 * 只有在setcommitoncheckpoint的情况下才能够保障至少一次的消费语义，在两次commit之间的数据会被重复消费
 */
public class KafkaSourceCommitTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.5:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hello");

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), props);

        env.addSource(consumer011).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>(value, value);
            }
        }).keyBy(0).map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> value) throws Exception {
                Thread.sleep(10000);
                return value.f1;
            }
        }).print("commit with setting test");

        env.execute();
    }
}
