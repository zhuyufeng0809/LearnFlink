package chapter4;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.hash.BloomFilter;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
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
        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, 11));
        list.add(new Tuple2<>(1, 22));
        list.add(new Tuple2<>(3, 33));
        list.add(new Tuple2<>(5, 55));

        DataStream<Tuple2<Integer, Integer>> dataStream = env.fromCollection(list);

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = dataStream.keyBy(0);

        keyedStream.print("输出结果");

        env.execute("KeyByTemplate");
    }
}