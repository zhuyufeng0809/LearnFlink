package chapter8;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * @date 2021-09-16
 * @Description:
 */
public class TableConvertDataStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setParallelism(1);

        DataStream<Tuple3<Long, String, Integer>> order = env.fromCollection(Arrays.asList(
                new Tuple3<>(1L, "手机", 1899),
                new Tuple3<>(1L, "电脑", 8888),
                new Tuple3<>(3L, "平板", 899)));

        tableEnv.registerDataStream("table_order", order, "user,product,amount");

        Table resultRow = tableEnv
                .sqlQuery("SELECT user,product,amount FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultRow, Row.class).print("Row Type: ");

        Table resultAtomic = tableEnv
                .sqlQuery("SELECT product FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultAtomic, String.class).print("Atomic Type: ");

        Table resultTuple = tableEnv
                .sqlQuery("SELECT product,amount FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultTuple, Types.TUPLE(Types.STRING, Types.INT)).print("Tuple Type: ");

        Table resultPojo = tableEnv
                .sqlQuery("SELECT user,product,amount FROM table_order WHERE amount < 3000");
        tableEnv.toAppendStream(resultPojo, OrderBean.class).print("Pojo Type: ");

        DataStream<Tuple2<Boolean, Row>> retract = tableEnv.toRetractStream(resultRow, Row.class);
        retract.print("Retract Row Type: ");

        env.execute();
    }

    public class OrderBean {

        public Long user;
        public String product;
        public int amount;

        public OrderBean() {
        }

        public OrderBean(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}