package chapter4;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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
        //env.setParallelism(10);
        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
        list.add(new Tuple2<>(1, 11));
        list.add(new Tuple2<>(1, 22));
        list.add(new Tuple2<>(3, 33));
        list.add(new Tuple2<>(5, 55));

        DataStream<Tuple2<Integer, Integer>> dataStream = env.fromCollection(list);

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = dataStream.keyBy(0);

        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }

            @Override
            public void processElement(Tuple2<Integer, Integer> value, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {

            }
        });

        env.execute("KeyByTemplate");
    }
}