package chapter9;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-12
 * @Description:
 */
public class TableFunctionTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        DataStream<OrderBean> input = env.fromElements(
                new OrderBean(1L, "beer#intsmaze", 3),
                new OrderBean(1L, "flink#intsmaze", 4),
                new OrderBean(3L, "rubber#intsmaze", 2));

        tableEnv.registerDataStream("orderTable", input, "user,product,amount");

        tableEnv.registerFunction("splitFunction", new MyTableFunction());

        Table sqlLeftResult = tableEnv.sqlQuery("SELECT user,product,amount,word, length FROM orderTable LEFT JOIN LATERAL TABLE(splitFunction(product)) AS T(word, length) ON TRUE");

        //SELECT user,product,amount,word, length FROM orderTable LEFT JOIN LATERAL TABLE(splitFunction(product)) AS T(word, length) ON TRUE
        tableEnv.toRetractStream(sqlLeftResult, Row.class).print("LEFT JOIN ");

        env.execute("TableFunctionTemplate");
    }
}
