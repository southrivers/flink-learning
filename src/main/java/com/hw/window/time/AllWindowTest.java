package com.hw.window.time;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 在使用了window算子之后后续的窗口函数不可以使用richfunction https://blog.csdn.net/nazeniwaresakini/article/details/129400204
 */
public class AllWindowTest {

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
        // 获取任务的分区信息richfunction就可以办到，获取window相关的信息则需要全量函数才可以，猜测windowall会将数据发往下游算子的同一个分区来进行计算
        // 不同的窗口会发往不同的下游算子进行计算，所以下游并行度依然可以大于1
        mapStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                        .reduce(new ReduceFunction<SensorReading>() {
                            @Override
                            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
//                                int index = getRuntimeContext().getIndexOfThisSubtask();
                                String s = sensorReading.getSensorId() + t1.getSensorId();
                                return new SensorReading(s, t1.getTimestamp(), t1.getTemp());
                            }
                        }).print("allwindow-test");
        env.execute();
    }
}
