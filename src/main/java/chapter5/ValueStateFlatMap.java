package chapter5;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-22
 * @Description:
 */
public class ValueStateFlatMap{

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RichSourceFunction<Tuple2<Integer, Integer>>() {

            private static final long serialVersionUID = 1L;

            private int counter = 0;

            @Override
            public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<>(counter % 5, counter));
                    System.out.println("send data :" + counter % 5 + "," + counter);
                    counter++;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
            }
        });

        KeyedStream<Tuple2<Integer, Integer>, Tuple> keyedStream = inputStream.keyBy(0);

        keyedStream.flatMap(new RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

            public transient ReducingState<Tuple2<Integer, Integer>> reducingState;

            @Override
            public void open(Configuration config) {
                ReducingStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                        new ReducingStateDescriptor<>(
                                "ReducingStateFlatMap",
                                (value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1),
                                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                                }));
                reducingState = getRuntimeContext().getReducingState(descriptor);
            }

            @Override
            public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                reducingState.add(input);
                out.collect(reducingState.get());
            }

        }).print();

        env.execute("Intsmaze ValueStateFlatMap");
    }
}

