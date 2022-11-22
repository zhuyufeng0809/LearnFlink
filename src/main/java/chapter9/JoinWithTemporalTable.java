package chapter9;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-15
 * @Description:
 */
public class JoinWithTemporalTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<RateBean> rateList = new ArrayList<>();
        rateList.add(new RateBean(1, "US Dollar", 102, "2019-07-28 09:00:00"));
        rateList.add(new RateBean(2, "Euro", 114, "2019-07-28 09:20:00"));
        rateList.add(new RateBean(3, "Yen", 1, "2019-07-28 09:30:00"));
        rateList.add(new RateBean(4, "Euro", 116, "2019-07-28 10:45:00"));
        rateList.add(new RateBean(5, "Euro", 119, "2019-07-28 11:15:00"));
        rateList.add(new RateBean(6, "Pounds", 108, "2019-07-28 11:49:00"));

        DataStream<RateBean> ratesHistory = env.fromCollection(rateList);
        ratesHistory = ratesHistory.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<RateBean>() {
            @Override
            public long extractTimestamp(RateBean element, long previousElementTimestamp) {
                return element.getTime().getTime();
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }
        });

        Table table = tEnv.fromDataStream(ratesHistory, "id,currency,time.rowtime,rate");
        tEnv.registerTable("RatesHistory", table);

        TemporalTableFunction fun = table.createTemporalTableFunction("time", "currency");
        tEnv.registerFunction("Rates", fun);

        List<Order> orderList = new ArrayList<>();
        orderList.add(new Order(1, "US Dollar", "2019-07-28 09:00:00", 2));
        orderList.add(new Order(2, "Euro", "2019-07-28 09:00:00", 10));
        orderList.add(new Order(3, "Yen", "2019-07-28 09:40:00", 30));
        orderList.add(new Order(4, "Euro", "2019-07-28 11:40:00", 30));

        DataStream<Order> orders = env.fromCollection(orderList);
        orders = orders.assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<Order>() {
                    @Override
                    public long extractTimestamp(Order element, long previousElementTimestamp) {
                        return element.getTime().getTime();
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(System.currentTimeMillis());
                    }
                });

        tEnv.registerDataStream("Orders", orders, "id,currency,orderTime.rowtime,amount");

        Table sqlResult = tEnv.sqlQuery("SELECT o.id,r.id,o.amount * r.rate AS amount " +
                "FROM Orders AS o,LATERAL TABLE (Rates(o.orderTime)) AS r " +
                "WHERE r.currency = o.currency");

        tEnv.toAppendStream(sqlResult, Row.class).print();

        env.execute();
    }
}
