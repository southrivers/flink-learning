package com.hw.wm;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * watermark功能测试，如下为kafka测试数据，在接收到第一条数据的时候，其对应的窗口就是195~200，
 * 此时会开一个桶用来接收数据，接下俩在接收到193~194的数据的时候，会继续开一个桶，不过这个桶是之前的
 * 一个窗口对应的桶，如果当前接收到一条新的数据，这个数据和当前已经接收到的数据的最大事件时间已经超出了
 * maxoutoforderness的话，这个数据直接就会被丢弃（当然可以通过设置允许延迟时间，让其增量更新，也可以设置旁路输出），
 * todo 也就是说当前分区：接收到的最大的时间戳-允许迟到的时间决定了接收的数据要不要丢掉，还是说需要再开一个桶
 *
 * >sensor_1,1547718199,31.8
 * >sensor_1,1547718199,31.8
 * >sensor_1,1547718199,31.8
 * >sensor_1,1547718198,31.8
 * >sensor_1,1547718198,31.8
 * >sensor_1,1547718199,31.8
 * >sensor_1,1547718194,23.8
 * >sensor_1,1547718193,23.7
 * >sensor_1,1547718204,66.8
 * >sensor_1,1547718205,35.4
 * >sensor_1,1547718210,43.6
 *
 */
public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试并行度为1的情况下watermark的情况
        env.setParallelism(1);
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
        });

        /**
         * 这里timewindow在生成window的时候，会去判断系统是事件时间还是处理时间，如果是系统时间就直接获取当前系统信息，如果是事件时间，
         * 就直接获取事件里面的时间，最终计算窗口的逻辑都是一样的，可以认为是当前的时间（事件时间或者当前系统时间）对窗口大小取整就是时间窗口的起始
         * 事件，因此对于事件时间来说其是可以同时开多个窗口的，而且允许先开后面的窗口，再开前面的窗口 TODO 这个开窗和并行度有什么关系没有，
         * todo 比如windowall或者keywindow
         */
        SingleOutputStreamOperator<String> processStream = mapSource.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction<SensorReading, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<SensorReading> values, Collector<String> out) throws Exception {
                int count = 0;
                for (SensorReading value : values) {
                    count ++;
                }
                out.collect(String.valueOf(count));
            }
        });
        processStream.print("window-all-watermark");
        env.execute();
    }
}
