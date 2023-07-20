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

public class CountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.5:9092");
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
                return new Tuple2<String, String>(split[0], split[1]);
            }
            // countwindow是根据相同key的个数进行的计算，这里将tuple和partition以及offset关联起来了，因此永远不可能收集到相同的两条数据，如果将window的窗口改为1的话就可以运行了
        }).countWindowAll(2).process(new ProcessAllWindowFunction<Tuple2<String, String>, String, GlobalWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<Tuple2<String, String>, String, GlobalWindow>.Context context, Iterable<Tuple2<String, String>> elements, Collector<String> out) throws Exception {
                StringJoiner stringJoiner = new StringJoiner("");
                for (Tuple2<String, String> element : elements) {
                    stringJoiner.add(element.f0).add(element.f1);
                }
                out.collect(stringJoiner.toString());
            }
        }).print("count-window");
        env.execute();
    }
}
