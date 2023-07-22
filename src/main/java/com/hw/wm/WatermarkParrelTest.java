package com.hw.wm;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.StringJoiner;

/**
 * watermark下游开window之后进行数据的处理，和上游的分区是相关联的，针对上游同一个分区来说，watermark选择当前分区最大的watermark（最大时间戳-乱序时间）
 * 不过因为window之后进行数据处理的函数接受了多个上游分区的数据，因此这个处理函数会选择分区之间最小的watermark决定当前处理任务分区的watermark来决定是否
 * 触发窗口计算。watermark是针对partition级别的，并不是针对key级别的。
 *
 *
 * 如下为测试数据
 * >sensor_1,1547718199,31.8
 * >sensor_1,1547718209,34.5
 * >sensor_1,1547718210,67.8
 * >sensor_2,1547718209,34.5
 * >sensor_2,1547718210,32.5 -- 这里也可以是sensor_1，因为发送到kafka里面的数据在缺失key的情况下是采用roundrobin的方法来的
 * >sensor_3,1547718210,34.5
 * >sensor_4,1547718210,34.5
 */

public class WatermarkParrelTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // watermark和key应该没有关系，应该是分区级别的数据流信息
        // 这里将并行度和kafka的并行度调成一致 TODO 这里因为watermark的下游是windowall的方式，也就是说所有的数据会被发往第一个分区，
        // TODO 下游的apply算子需要在收到上游两个分区的数据的watermark，并且会选取较小的watermark作为当前算子的watermark
        env.setParallelism(2);
        // 使用watermark需要设置使用事件时间
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
            // 如果要自己实现watermark的机制的话，需要定义两个字段，最大乱序时间用于生成watermark，还有就是当前最大事件时间，用于决定当前算子的最大watermark
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000;
            }
        }).setParallelism(4);

        /**
         * 这里timewindow在生成window的时候，会去判断系统是事件时间还是处理时间，如果是系统时间就直接获取当前系统信息，如果是事件时间，
         * 就直接获取事件里面的时间，最终计算窗口的逻辑都是一样的，可以认为是当前的时间（事件时间或者当前系统时间）对窗口大小取整就是时间窗口的起始
         * 事件，因此对于事件时间来说其是可以同时开多个窗口的，而且允许先开后面的窗口，再开前面的窗口 TODO 这个开窗和并行度有什么关系没有，
         * todo 比如windowall或者keywindow
         */
        SingleOutputStreamOperator<Tuple2<String, SensorReading>> map = mapSource.map(new MapFunction<SensorReading, Tuple2<String, SensorReading>>() {
            @Override
            public Tuple2<String, SensorReading> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getSensorId(), sensorReading);
            }
        });
        map.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction<Tuple2<String, SensorReading>, Integer, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Tuple2<String, SensorReading>> values, Collector<Integer> out) throws Exception {
                int count = 0;
                for (Tuple2<String, SensorReading> value : values) {
                    count ++;
                }
                out.collect(count);
            }
        }).print("parrel-all-window-watermark");
        env.execute();
    }
}
