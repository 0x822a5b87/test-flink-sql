package com.xxx.sql.score;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author 0x822a5b87
 */
public class StreamScoreApp {

    private static void streamWindow(StreamExecutionEnvironment env,
                                     StreamTableEnvironment tableEnv) throws IOException, InterruptedException {
        // 生成数据源
        List<String>       sql   = Files.readAllLines(Paths.get("src/main/resources/score.csv"));
        DataStream<String> input = env.fromCollection(sql)
                                        .assignTimestampsAndWatermarks(new StringWatermarkAssigner());

        DataStream<PlayerData> topInput = input.map((MapFunction<String, PlayerData>) PlayerData::new)
                                               .assignTimestampsAndWatermarks(new WatermarkAssigner())
                                               .setParallelism(1);

        tableEnv.createTemporaryView("score", topInput,
                                     "season, player, playNum, firstCourt, time, assists, steals, blocks, scores, ts.rowtime");

        Table queryResult = tableEnv.sqlQuery("select"
                                              + " HOP_START (ts, INTERVAL '1' SECOND, INTERVAL '2' SECOND) as hopStart,"
                                              + " HOP_END   (ts, INTERVAL '1' SECOND, INTERVAL '2' SECOND) as hopEnd,"
                                              + "player, count(scores) as num, sum(scores) as total"
                                              + " from score"
                                              + " GROUP BY HOP( ts , INTERVAL '1' SECOND, INTERVAL '2' SECOND), player"
        );

        DataStream<Result> ds = tableEnv.toAppendStream(queryResult, Result.class);
        ds.print("result").setParallelism(1);
    }

    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env      = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment     tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        streamWindow(env, tableEnv);

        env.execute("stream score");

        Thread.sleep(1000 * 60);
    }
}
