package chapter8;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-09-09
 * @Description:
 */
public class StreamTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Tuple3<Long, String, Double>> order = env.fromCollection(Arrays.asList(
                new Tuple3<>(1L, "手机", 1899.00),
                new Tuple3<>(1L, "电脑", 8888.00),
                new Tuple3<>(3L, "平板", 899.99)));

        tableEnv.registerDataStream("table_order", order, "user,product,amount");

        Table a = tableEnv.sqlQuery("select user,count(1) from table_order group by user");

        tableEnv.toRetractStream(a, Row.class).print();
    }
}