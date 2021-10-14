package com.xxx.sql.tutorial;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author 0x822a5b87
 */
public class InsertingIntoTableApp {

    private static final String SQL_CREATE_TABLE_SERVER_LOGS = "CREATE TABLE server_logs ( \n"
                                                               + "    client_ip STRING,\n"
                                                               + "    client_identity STRING, \n"
                                                               + "    userid STRING, \n"
                                                               + "    user_agent STRING,\n"
                                                               + "    log_time TIMESTAMP(3),\n"
                                                               + "    request_line STRING, \n"
                                                               + "    status_code STRING, \n"
                                                               + "    size INT\n"
                                                               + ") WITH (\n"
                                                               + "  'connector' = 'faker', \n"
                                                               + "  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',\n"
                                                               + "  'fields.client_identity.expression' =  '-',\n"
                                                               + "  'fields.userid.expression' =  '-',\n"
                                                               + "  'fields.user_agent.expression' = '#{Internet.userAgentAny}',\n"
                                                               + "  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',\n"
                                                               + "  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\\.html|/login\\.html|/prod\\.html|cart\\.html|/order\\.html){1}''} #{regexify ''(HTTP/1\\.1|HTTP/2|/HTTP/1\\.0){1}''}',\n"
                                                               + "  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}',\n"
                                                               + "  'fields.size.expression' = '#{number.numberBetween ''100'',''10000000''}'\n"
                                                               + ")";

    private static final String SQL_CREATE_TABLE_CLIENT_ERRORS = "CREATE TABLE client_errors (\n"
                                                                 + "  log_time TIMESTAMP(3),\n"
                                                                 + "  request_line STRING,\n"
                                                                 + "  status_code STRING,\n"
                                                                 + "  size INT\n"
                                                                 + ")\n"
                                                                 + "WITH (\n"
                                                                 + "  'connector' = 'blackhole'\n"
                                                                 + ")";

    private static final String SQL_INSERT_INTO = "INSERT INTO client_errors\n"
                                                  + "SELECT \n"
                                                  + "  log_time,\n"
                                                  + "  request_line,\n"
                                                  + "  status_code,\n"
                                                  + "  size\n"
                                                  + "FROM server_logs\n"
                                                  + "WHERE \n"
                                                  + "  status_code SIMILAR TO '4[0-9][0-9]'";

    private static final String SQL_SELECT = "select * from client_errors";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env      = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment     tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql(SQL_CREATE_TABLE_SERVER_LOGS);
        tableEnv.executeSql(SQL_CREATE_TABLE_CLIENT_ERRORS);
        tableEnv.executeSql(SQL_INSERT_INTO);
        TableResult selectResult = tableEnv.executeSql(SQL_SELECT);
        selectResult.print();
        env.execute("insert into");
    }
}
