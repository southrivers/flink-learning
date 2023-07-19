package com.hw.window.time;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class TimeWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> sourceStream = env.socketTextStream("192.168.28.3", 7777);
        DataStream<SensorReading> mapStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        // 1、首先需要记住的是flink默认的时间窗口是processtime，如果需将将窗口设置为eventtime的话，需要单独的配置
        // 2、一般是需要先对datastream进行keyBy操作，然后应用window，最后加上windowfunction，
        // 在上述的流程中，datastream的转换的过程是：datastream -> keyedstream -> windowstream -> datastream
        // 以下对windowfunction进行测试，
        // 算子小结：区别于在非window上的operator算子，windowfunction仅针对当前window是有效的，下一个window的话不会留存上一个window的信息
        // thumbling window ：翻转窗口无论数据到来多少条到达窗口就会触发
        /**
         * 1、以下都是使用的增量聚合函数
         */
        /*mapStream.keyBy("sensorId")
                        .timeWindow(Time.seconds(20))
                                .reduce(new ReduceFunction<SensorReading>() {
                                    @Override
                                    public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                                        return new SensorReading(sensorReading.getSensorId(), Math.max(sensorReading.getTimestamp(), t1.getTimestamp()), Math.max(sensorReading.getTemp(), t1.getTemp()));
                                    }
                                }).print("thumble-reduce");*/
        // 上面的翻转窗口类似于slide窗口大小和window窗口大小一致，这里的slide可以认为是滑动的频率

        // 1.1 reduce计算
        /*mapStream.keyBy("sensorId")
                .timeWindow(Time.seconds(20), Time.seconds(10))
                .reduce(new ReduceFunction<SensorReading>() {

                    @Override
                    public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {

                        return new SensorReading(sensorReading.getSensorId(), Math.max(sensorReading.getTimestamp(), t1.getTimestamp()), Math.max(sensorReading.getTemp(), t1.getTemp()));
                    }
                }).print("slide-reduce");*/

        // TODO 如何获取窗口的信息？？？？ reduce和aggregate属于增量聚合函数，要获取窗口信息等参数需要使用全窗口函数，这种全窗口函数会积攒一批数据，然后一次进行计算
        // slide打印数据有时候会很奇怪，原因是每slide窗口滑动一次

        // 1.2、aggregate计算 可以用来求平均值，当然，在求的时候需要将信息记录到accumulator里面
        // TODO 这里打印输出的次数和slide的大小是有关系的，假定时间轴从左向右，我们认为数据再10s的时候来了一批，那么其开始的窗口其实是在-10s的时候，也就是说10s是其结束的
        // todo 时间窗口，因此在接下来的过程中每次向右滑动2s，这样10s到来的数据在窗口向右滑动10次之后会彻底消失在当前的窗口，因此也就不打印了
        // TODO thumble 窗口就不存在这种问题，因为彼此之间不重叠，因此数据只会落到一个窗口里面
        /*mapStream.keyBy("sensorId")
                .timeWindow(Time.seconds(20), Time.seconds(2))
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Long, Double>, SensorReading>() {

                    @Override
                    public Tuple2<Long, Double> createAccumulator() {
                        return new Tuple2<Long, Double>(0L, 0.0);
                    }

                    @Override
                    public Tuple2<Long, Double> add(SensorReading sensorReading, Tuple2<Long, Double> integerIntegerTuple2) {
                        long ts = Math.max(sensorReading.getTimestamp(), integerIntegerTuple2._1);
                        Double tem = Math.max(sensorReading.getTemp(), integerIntegerTuple2._2);
                        return new Tuple2<Long, Double>(ts, tem);
                    }

                    @Override
                    public SensorReading getResult(Tuple2<Long, Double> integerIntegerTuple2) {
                        // 获取最终的运行结果
                        SensorReading reading = new SensorReading();
                        reading.setTemp(integerIntegerTuple2._2);
                        reading.setTimestamp(integerIntegerTuple2._1);
                        return reading;
                    }

                    *//**
                     * todo 这里的merge不清楚有什么作用，待分析，说是只有在session窗口的时候才有用
                     *
                     * @param integerIntegerTuple2
                     * @param acc1
                     * @return
                     *//*
                    @Override
                    public Tuple2<Long, Double> merge(Tuple2<Long, Double> integerIntegerTuple2, Tuple2<Long, Double> acc1) {
                        return null;
                    }
                }).print("slide-aggregate");*/

        // TODO 全窗口函数是等待数据全部到来
        // 也可以直接对datastream应用window开窗操作这样的话，流的转换过程是datastream -> allwindowstream -> datastream
        // todo： allwindowfunction是在开窗之后使用apply或者process方法来实现
        /*mapStream.keyBy("sensorId").timeWindow(Time.seconds(10), Time.seconds(2))
                        .apply(new WindowFunction<SensorReading, Tuple4<String, Long, Double, Long>, Tuple, TimeWindow>() {
                            // 这里的tuple是传递上面按照key值进行keyBy的key，之所以是一个tuple而不是string类型的是因为keyBy可以传递多个参数
                            // 因此使用tuple来封装key的话更具有通用性
                            @Override
                            public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input,
                                              Collector<Tuple4<String, Long, Double, Long>> out) throws Exception {
                                String id = null;
                                long timestamp = 0;
                                double temp = 0.0;
                                Iterator<SensorReading> iterator = input.iterator();
                                // input.iterator().hasNext()的话永远不会有输出了，需要固化一个变量然后迭代才可以
                                while (iterator.hasNext()) {
                                    SensorReading next = iterator.next();

                                    if (null == id) {
                                        id = next.getSensorId();
                                    }
                                    timestamp = Math.max(timestamp, next.getTimestamp());
                                    temp = Math.max(temp, next.getTemp());
                                }
                                out.collect(new Tuple4<String, Long, Double, Long>(id + "#" + tuple.toString(), timestamp, temp, window.getEnd()));
                            }
                        }).print("allwindow-apply");*/
        // 打印窗口打印多次原因有二：1、分区了，不同的数据分不到不同的分区 2、滑动窗口小于窗口大小，因为是按照窗口的结束时间来划分数据的，因此数据同一个分区的同一条数据会被多个窗口拥有而使用
        // 接下来使用process方法作为全窗口函数来处理数据
        mapStream.keyBy("sensorId").timeWindow(Time.seconds(10), Time.seconds(2))
                .process(new ProcessWindowFunction<SensorReading, SensorReading, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<SensorReading, SensorReading, Tuple, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<SensorReading> out) throws Exception {
                        Iterator<SensorReading> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            out.collect(iterator.next());
                        }
                    }
                }).print("allwindow-apply");
        env.execute();
    }
}
