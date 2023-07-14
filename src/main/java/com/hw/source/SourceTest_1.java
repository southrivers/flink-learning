package com.hw.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest_1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> sourceStream = env.readTextFile("D:\\workspace\\github\\test-flink\\flink-test\\src\\main\\resources\\sensorreading.txt");
        sourceStream.print("source");

        env.execute("test");
    }
}
