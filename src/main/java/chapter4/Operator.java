package chapter4;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-02
 * @Description:
 */
public class Operator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        list.add(new Tuple2<>(1, 11));
        list.add(new Tuple2<>(1, 22));
        list.add(new Tuple2<>(3, 33));
        list.add(new Tuple2<>(5, 55));

        DataStream<Tuple2<Integer, Integer>> dataStream = env.fromCollection(list);

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = dataStream.keyBy("f0");

        keyedStream.print("输出结果");

        env.execute("KeyByTemplate");
    }
}