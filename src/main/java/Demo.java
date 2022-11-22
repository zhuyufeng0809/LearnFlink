import chapter9.ClickBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-04-18
 * @Description:
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

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

        DataStream<ClickBean> streamSource = env.fromCollection(clicksData);

        tableEnv.registerDataStream("Clicks", streamSource, "id,user,url,time");

        Table table = tableEnv.sqlQuery("SELECT user AS name, count(url) AS number FROM Clicks GROUP BY user");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }
}
