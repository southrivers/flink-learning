package com.hw.window.time;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountWindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> sourceStream = env.socketTextStream("192.168.28.3", 7777);
        DataStream<SensorReading> mapStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        /**
         * TODO slide的滑动窗口指代的是每来多少条数据就执行一次计算，size的含义是窗口中最大容纳的数量，这一点很重要，并不是来了size条数据之后向右滑动一定的距离
         */
        // 这里的窗口是计数窗口，只有等到足够的数据量的时候才会触发计算
        mapStream.keyBy("sensorId").countWindow(5, 2)
                        .process(new ProcessWindowFunction<SensorReading, Tuple2<String, Integer>, Tuple, GlobalWindow>() {
                            @Override
                            public void process(Tuple tuple, ProcessWindowFunction<SensorReading, Tuple2<String, Integer>, Tuple, GlobalWindow>.Context context,
                                                Iterable<SensorReading> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                                String id = null;
                                int sum = 0;
                                int count = 0;
                                for (SensorReading element : elements) {
                                    if (null == id) {
                                        id = element.getSensorId();
                                    }
                                    sum += element.getTemp();
                                    count ++;
                                }
                                out.collect(new Tuple2<String, Integer>(id, count));
                            }
                        }).print("count-process");

        env.execute();
    }
}
