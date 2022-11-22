package chapter9;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-12
 * @Description:
 */
public class AggregateFunctionTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        List<Row> dataList = new ArrayList<>();
        dataList.add(Row.of("张三", "可乐", 20.0D, 4));
        dataList.add(Row.of("张三", "果汁", 10.0D, 4));
        dataList.add(Row.of("李四", "咖啡", 10.0D, 2));

        DataStream<Row> rowDataSource = env.fromCollection(dataList);

        tEnv.registerDataStream("orders", rowDataSource, "user,name,price, num");

        tEnv.registerFunction("custom_aggregate", new MyAccumulator());

        Table sqlResult = tEnv.sqlQuery("SELECT user, custom_aggregate(price, num)  FROM orders GROUP BY user");

        DataStream<Tuple2<Boolean, Row>> result = tEnv.toRetractStream(sqlResult, Row.class);
        result.print("result");
        env.execute();
    }
}
