package com.hw.source;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class SourceTest_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        System.out.println(env.getParallelism());
        Properties prop = new Properties();
        // 这里kafka使用flink自带的序列化、反序列化器，不要使用kafka里面的StringDescribleSchema
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        prop.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000);
        prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        // 如果要读取当前消息的offset，需要自定义反序列化器才可以，默认的simpleStringschema是无法获取对应消息的offset的，因为这种方式只解析value，可以自定义
        // 反序列化器，将offset封装到消息中即可：https://blog.csdn.net/qq_26502245/article/details/112391375
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), prop);

        // FIXME
        // https://stackoverflow.com/questions/54585740/commit-kafka-offsets-manually-in-flink  这个博客有手动commit kafka offset的示例代码，下来需要查看
        //        flinkKafkaConsumer011.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> kafkaStream = env.addSource(flinkKafkaConsumer011);
        // 设置消费kafka的offset值：https://blog.csdn.net/gxd520/article/details/92768308
        kafkaStream.print();

        env.execute();
    }
}
