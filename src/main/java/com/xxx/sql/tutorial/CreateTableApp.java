package com.xxx.sql.tutorial;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 0x822a5b87
 */
public class CreateTableApp {

    private static final String SQL_CREATE_TABLE = "CREATE TABLE orders (\n"
                                                   + "    order_uid  BIGINT,\n"
                                                   + "    product_id BIGINT,\n"
                                                   + "    price      DECIMAL(32, 2),\n"
                                                   + "    order_time TIMESTAMP(3)\n"
                                                   + ") WITH (\n"
                                                   + "    'connector' = 'datagen'\n"
                                                   + ")";

    private static final String SQL_SELECT = "SELECT * FROM orders";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment   env      = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(SQL_CREATE_TABLE);
        TableResult selectResult = tableEnv.executeSql(SQL_SELECT);
        selectResult.print();
        env.execute("create table");
    }
}
