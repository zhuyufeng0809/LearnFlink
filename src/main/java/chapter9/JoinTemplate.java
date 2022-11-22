package chapter9;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-04
 * @Description:
 */
public class JoinTemplate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

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
        DataStream<ClickBean> clicksStream = env.fromCollection(clicksData);
        clicksStream = clicksStream.map((MapFunction<ClickBean, ClickBean>) value -> {
            Thread.sleep(1000);
            return value;
        });
        tEnv.registerDataStream("Clicks", clicksStream, "user,time,url");

        List<Person> personData = new ArrayList<>();
        personData.add(new Person("张三", 38, "上海"));
        personData.add(new Person("张三", 45, "深圳"));
//        personData.add(new Person("赵六", 18, "天津"));
        Collections.shuffle(personData);
        DataStream<Person> personStream = env.fromCollection(personData);
        personStream = personStream.map((MapFunction<Person, Person>) value -> {
            Thread.sleep(10000);
            return value;
        });
        tEnv.registerDataStream("Person", personStream, "name,age,city");


//        tEnv.toAppendStream(tEnv.sqlQuery("SELECT * FROM Clicks INNER JOIN Person ON name=user"), Row.class).print("INNER JOIN");
        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks LEFT JOIN Person ON name=user"), Row.class).print("LEFT JOIN");
//        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks RIGHT JOIN Person ON name=user"), Row.class).print("RIGHT JOIN");
//        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks FULL JOIN Person ON name=user"), Row.class).print("FULL JOIN");

        env.execute();
    }
}
