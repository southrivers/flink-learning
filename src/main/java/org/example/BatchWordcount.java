package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordcount {
    public static void main(String[] args) throws Exception {

        // 构造环境变量，这里的环境变量可以看做是一个工具集合
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> source = env.readTextFile("D:\\workspace\\github\\test-flink\\flink-test\\src\\main\\resources\\hello.txt");
        FlatMapOperator<String, Tuple2<String, Integer>> wordMap = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });
        // 这里因为是批处理数据，因此是可以使用group的，因为所有数据都已经拿到了，相反对于流数据则不可以使用group，而只可以使用key
        wordMap.groupBy(0).sum(1).print();
        // 因为是批处理数据，因此这里不需要单独的调用env.execute，env.execute更多的是启动一个常驻的任务，用来接受上游发过来的数据
    }
}
