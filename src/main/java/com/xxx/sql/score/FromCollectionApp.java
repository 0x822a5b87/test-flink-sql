package com.xxx.sql.score;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author 0x822a5b87
 */
public class FromCollectionApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String>   stream = env.fromCollection(Arrays.asList("hello", "world", "!"));
        stream.print();
        env.execute("from collection");
    }
}
