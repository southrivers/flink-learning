package com.hw.state;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 基于状态实现温度跳变的检测
 */
public class TempJumpStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 状态存放在taskmanager上面，checkpoint数据存放在jobmanager的内存上
        env.setStateBackend(new MemoryStateBackend());
        // 状态存放在taskmanager上面，checkpoint数据存放在hdfs上
        env.setStateBackend(new FsStateBackend(""));
        // 状态存在rocksdb里面，checkpoint数据存放在hdfs上
        env.setStateBackend(new RocksDBStateBackend(""));

        // 开启检查点，这里的参数有两个，一个是interval代表多长时间做一次快照，另外是checkpointmode，有exactlyonce和atleastonce两种语义
        // 默认是exactlyonce语义
        env.enableCheckpointing(60000);

        // 因为checkpoint的过程是同步执行的，在远端的存储或者网络出现问题的情况下，会导致当前的operator对应的任务受阻，所以推荐设置超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000);

        // 代表了前一个checkpoint还没有做完，下一个checkpoint就已经开始了（两个checkpoint是不同的cb来实现的）
        // checkpoint的触发只要jobmanager发出一个指令给source就可以触发了，TODO 建议设置1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 两个checkpoint最小的时间间隔（这里的between并不是触发cb的保存，而是cb有一段保存时间，两个cb最小要间隔多久）
        // 限制checkpoint间隔留下来一段时间来处理数据，推荐小于窗口的时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // TODO 使用检查点来做状态的恢复，false的情况下会使用最近的checkpoint或者savepoint来进行恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);
        // 如果保存checkpoint失败了，整个任务task就失败了，0次就是不容忍checkpoint失败，可以设置这个值大于0
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);



        // 重启策略的配置
        // 这是不重启
        env.setRestartStrategy(RestartStrategies.noRestart());
        // 固定延迟重启：对应参数，尝试重启的次数，两次重启的间隔，这里的参数的含义代表最多尝试重启3次，重启不成功那么重启之间的时间间隔是10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        // 这里代表了在10s的时间内进行重启，每次重启间隔最少要2s，并且最多允许做3次重试
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(10), Time.seconds(2)));


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
        });

        mapSource.keyBy("sensorId").flatMap(new RichFlatMapFunction<SensorReading, String>() {
            ValueState<Double> lastTemp;

            @Override
            public void open(Configuration parameters) throws Exception {
                // State的定义伴随着一个statedescriptor
                lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("key-count", Double.class));
            }
            @Override
            public void flatMap(SensorReading sensorReading, Collector<String> collector) throws Exception {
                Double last = lastTemp.value();
                if (last != null) {
                    double v = Math.abs(sensorReading.getTemp() - last);
                    if (v > 10.0) {
                        collector.collect(sensorReading.toString());
                    }
                }
                lastTemp.update(sensorReading.getTemp());
            }
        }).print("state-count");

        env.execute();
    }
}
