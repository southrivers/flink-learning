package com.hw.window.count;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.StringJoiner;

public class WindowAllAfterKey {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.5:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "wes111");
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<String>("test", new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String s) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {

                return "partition: " + consumerRecord.partition() + ", offset: " + consumerRecord.offset() + "# value: " + new String(consumerRecord.value());
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        }, props);
        DataStreamSource<String> source = env.addSource(consumer011);

        source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split("#");
                return new Tuple2<String, String>(split[1], split[0]);
            }
            // keyBy之后可以使用windowAll其本质和不使用key的情况是一致的，只是keyBy之后会首先按照key值进行分区，每一个key值有对应的
        }).keyBy(0).countWindowAll(2).process(new ProcessAllWindowFunction<Tuple2<String, String>, String, GlobalWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<Tuple2<String, String>, String, GlobalWindow>.Context context, Iterable<Tuple2<String, String>> elements, Collector<String> out) throws Exception {
                StringJoiner joiner = new StringJoiner("");
                for (Tuple2<String, String> element : elements) {
                    joiner.add(element.f0).add(element.f1);
                }
                out.collect(joiner.toString());
            }
        }).print("windowAll after keyBy");

        env.execute();
    }
}
