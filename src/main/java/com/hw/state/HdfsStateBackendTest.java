package com.hw.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class HdfsStateBackendTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.5:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "hello");

        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), props);
        // 这里会在checkpoint的时候自动提交offset，能够保证至少一次消费的语义！！！！！！
        consumer011.setCommitOffsetsOnCheckpoints(true);
        // 结合上面的setcommitoncheckpoint为true，这一步会控制kafka提交的频率
        env.enableCheckpointing(1000);
        // 将状态后端设置为hdfs，TODO 需要有hdfs相关的依赖 rocksdb也可以进行设置，在重新启动的时候需要手动指定从哪个checkpoint进行状态的恢复
        env.setStateBackend(new FsStateBackend("hdfs://192.168.28.7:9000/wes/cbt"));
//        env.setStateBackend(new MemoryStateBackend());
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);

        env.addSource(consumer011).map(new MyMapper()).print("count-checkp");
        env.execute();
    }

    /**
     * 1、测试重启状态是否会保存
     * 2、测试并行度发生改变状态是否会合并
     */
    static class MyMapper implements MapFunction<String, Integer>, ListCheckpointed<Integer> {

        Integer count;

        @Override
        public Integer map(String value) throws Exception {
            // TODO 这里必须要判断状态的初始状态，如果直接++，不会报错也不会输出，只会hang住，kafka的offset也不会消费！！！！
            if (null == count) {
                count = 0;
            }
            count ++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer integer : state) {
                count += integer;
            }
        }
    }
}
