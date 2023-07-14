package com.hw.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * 通过这种方案可以获取到实时消费到kafka的位置，方案就是自定义反序列化器，这样可以获取丰富的资源信息
 */
public class SourceTest_Kafka_1 {
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
        FlinkKafkaConsumer011<String> flinkKafkaConsumer011 = new FlinkKafkaConsumer011<String>("sensor", new MyDeserialization(), prop);

        //TODO 这里因为类型擦除问题必须要加returns，需要好好的研究一下类型擦除带来的影响
        DataStream<String> kafkaStream = env.addSource(flinkKafkaConsumer011).returns(Types.STRING);
        kafkaStream.print();

        env.execute();
    }

    public static class MyDeserialization implements KafkaDeserializationSchema<String> {

        @Override
        public boolean isEndOfStream(String s) {
            return false;
        }

        @Override
        public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
            return consumerRecord.partition() + "," + consumerRecord.offset() + "," + new java.lang.String(consumerRecord.value());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return null;
        }
    }
}
