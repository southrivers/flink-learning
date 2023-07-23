package com.hw.state;

import com.hw.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class OperatorStateTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 测试并行度为1的情况下watermark的情况
//        env.setParallelism(1);
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
        });

        mapSource.map(new MyMapper()).print("state-count");

        env.execute();
    }

    // 实现了ListStatecheckpoint的接口就是ListState，这是operator state的实现方式，至于其他的uinonliststate状态
    // ListCheckpoint是checkpointfunction的一种简写，只支持均匀分布的ListState，不支持全量广播的unionListstate
    // 在这里我们看到的是MyMapper的一个简单类型的成员变量，不过实际上来说底层应该是对应了一个StateDescriptor
    static class MyMapper implements MapFunction<SensorReading, Integer> , ListCheckpointed<Integer> {
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count ++;
            return count;
        }

        // 这个过程在checkpoint的时候会将数据写回到检查点
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            // 这里是将变量直接定义成state了，其实更多的情况是将变量和state分开定义
            return Collections.singletonList(count);
        }

        // 这个是在故障恢复的时候，并行度发生调整的情况下，当前分区的任务会收到多个状态数据，这里就是并行度调整的时候
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer integer : state) {
                count += integer;
            }
        }
    }

    // 这里通过实现CheckpointFunction接口可以方便的获取和定义各种类型的状态，而ListCheckPointedFunction的话则尽用于ListState的一个接口
    // @Link https://blog.51cto.com/u_15127658/3510826
    static class MyMapper1 implements CheckpointedFunction, MapFunction<SensorReading, Integer> {

        // 用来持久化状态数据
        ListState<Integer> state;

        // 用来临时保存状态数据
        Integer count;

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // 设置快照的时候先清空上一次的快照数据
            state.clear();
            state.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<Integer>("local-state", Integer.class);
            state = context.getOperatorStateStore().getListState(descriptor);
            // 如果是restore的话，涉及到状态的重构
            if(context.isRestored()) {
                for (Integer integer : state.get()) {
                    count += integer;
                }
            }
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }
    }
}
