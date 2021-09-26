package com.xxx.sql.score;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @author 0x822a5b87
 */
public class ScoreSqlApp {

    public static void main(String[] args) throws Exception {

        // 获取执行环境
        ExecutionEnvironment  env      = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        // 生成数据源
        DataSet<String> input = env.readTextFile("src/main/resources/score.csv");
        input.print();
        DataSet<PlayerData> topInput = input.map((MapFunction<String, PlayerData>) s -> {
            String[] split = s.split(",");
            return new PlayerData(String.valueOf(split[0]),
                                  String.valueOf(split[1]),
                                  String.valueOf(split[2]),
                                  Integer.valueOf(split[3]),
                                  Double.valueOf(split[4]),
                                  Double.valueOf(split[5]),
                                  Double.valueOf(split[6]),
                                  Double.valueOf(split[7]),
                                  Double.valueOf(split[8])
            );
        });

        Table topScore = tableEnv.fromDataSet(topInput);
        tableEnv.registerTable("score", topScore);

        Table queryResult = tableEnv.sqlQuery("select player, count(season) as num"
                                              + " from score"
                                              + " group by player"
                                              + " order by num desc"
                                              + " limit 3");

        DataSet<Result> resultDataSet = tableEnv.toDataSet(queryResult, Result.class);
        resultDataSet.print();
    }
}
