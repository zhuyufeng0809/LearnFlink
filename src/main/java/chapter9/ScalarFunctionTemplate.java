package chapter9;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-12
 * @Description:
 */
public class ScalarFunctionTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Configuration conf = new Configuration();
        conf.setString("sleepTime", "2");
        env.getConfig().setGlobalJobParameters(conf);

        List<Row> data = new ArrayList<>();
        data.add(Row.of("intsmaze", "2019-07-28 12:00:00", ".../intsmaze/"));
        data.add(Row.of("Flink", "2019-07-25 12:00:00", ".../intsmaze/"));

        DataStream<Row> orderRegister = env.fromCollection(data);
        tableEnv.registerDataStream("testSUDF", orderRegister, "user,visit_time,url");
        tableEnv.registerFunction("custom_Date", new MyScalarFunction());

        Table sqlResult = tableEnv.sqlQuery("SELECT user, custom_Date( visit_time ),custom_Date( visit_time ,2) FROM testSUDF");
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(sqlResult, Row.class);

        result.print("result");
        env.execute();
    }
}
