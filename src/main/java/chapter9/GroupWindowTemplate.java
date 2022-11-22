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
import java.util.Collections;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-10-28
 * @Description:
 */
public class GroupWindowTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        ArrayList<ClickBean> clicksData = new ArrayList<>();
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
        Collections.shuffle(clicksData);
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

        String sqlQuery = "SELECT user AS name,\" +\n" +
                "                \"count(url) \" +\n" +
                "                \",TUMBLE_START(VisitTime, INTERVAL '1' HOUR) \" +\n" +
                "                \",TUMBLE_ROWTIME(VisitTime, INTERVAL '1' HOUR) \" +\n" +
                "                \",TUMBLE_END(VisitTime, INTERVAL '1' HOUR)  \" +\n" +
                "                \"FROM Clicks \" +\n" +
                "                \"GROUP BY TUMBLE(VisitTime, INTERVAL '1' HOUR), user ";

        Table table = tableEnv.sqlQuery(sqlQuery);
        DataStream<Row> resultStream = tableEnv.toAppendStream(table, Row.class);

        resultStream.print();

        env.execute();
    }
}
