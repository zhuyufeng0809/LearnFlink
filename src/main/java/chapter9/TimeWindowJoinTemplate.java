package chapter9;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-04
 * @Description:
 */
public class TimeWindowJoinTemplate {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        List<ClickBean> clicksData = new ArrayList<>();
        clicksData.add(new ClickBean(1, "张三", "./intsmaze", "2019-07-28 12:00:00"));
        clicksData.add(new ClickBean(2, "李四", "./flink", "2019-07-28 12:05:05"));
        clicksData.add(new ClickBean(3, "张三", "./intsmaze", "2019-07-28 12:08:08"));
        clicksData.add(new ClickBean(4, "张三", "./sql", "2019-07-28 12:30:00"));
        clicksData.add(new ClickBean(5, "李四", "./intsmaze", "2019-07-28 13:01:00"));
        clicksData.add(new ClickBean(6, "王五", "./flink", "2019-07-28 13:20:00"));
        clicksData.add(new ClickBean(7, "王五", "./sql", "2019-07-28 13:30:00"));
        clicksData.add(new ClickBean(8, "张三", "./intsmaze", "2019-07-28 14:10:00"));
        clicksData.add(new ClickBean(9, "王五", "./flink", "2019-07-28 14:20:00"));
        clicksData.add(new ClickBean(10, "李四", "./intsmaze", "2019-07-28 14:30:00"));
        clicksData.add(new ClickBean(11, "李四", "./sql", "2019-07-28 14:40:00"));
        DataStream<ClickBean> dataStream = env.fromCollection(clicksData);

        dataStream = dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ClickBean>() {

            @Override
            public long extractTimestamp(ClickBean element, long previousElementTimestamp) {
                return element.getTime();
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }
        });


        tableEnv.registerDataStream("Clicks", dataStream, "id,user,VisitTime.rowtime,url");

        String sqlQuery =
                "SELECT temp.name, " +
                        "temp.minId," +
                        "id," +
                        "temp.n," +
                        "url," +
                        "temp.betweenStart," +
                        "temp.betweenTime" +
                        " FROM (" +
                        "SELECT user as name, " +
                        "count(url) as n ," +
                        "min(id) as minId," +
                        "TUMBLE_ROWTIME(VisitTime, INTERVAL '1' HOUR) as betweenTime," +
                        "TUMBLE_START(VisitTime, INTERVAL '1' HOUR) as betweenStart  " +
                        "FROM Clicks " +
                        "GROUP BY TUMBLE(VisitTime, INTERVAL '1' HOUR), user"
                        + ") temp LEFT JOIN Clicks ON temp.minId=Clicks.id " +
                        "AND Clicks.VisitTime <= temp.betweenTime AND Clicks.VisitTime >= temp.betweenTime - INTERVAL '1' HOUR";

        Table table = tableEnv.sqlQuery(sqlQuery);

        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();
    }
}
