package chapter5;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-22
 * @Description:
 */
public class ValueStateFlatMap{

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);
        env.setParallelism(2);
        String path = "file:///Users/zhuyufeng/IdeaProjects/LearnFlink/src/main/resources/";
        FsStateBackend stateBackend = new FsStateBackend(path);
        env.setStateBackend(stateBackend);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));

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

            public transient ValueState<Tuple2<Integer, Integer>> valueState;

            @Override
            public void open(Configuration config) {
                ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor =
                        new ValueStateDescriptor<>(
                                "ValueStateFlatMap",
                                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                                }));
                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                Tuple2<Integer, Integer> currentSum = valueState.value();

                if (input.f1 == 10) {
                    int i = 1 /0;
                }

                if (currentSum == null) {
                    currentSum = input;
                } else {
                    currentSum.f1 = currentSum.f1 + input.f1;
                    currentSum.f0 = currentSum.f0 + input.f0;
                }
                out.collect(input);
                valueState.update(currentSum);
                System.out.println(Thread.currentThread().getName() + " currentSum after:" + valueState.value() + ",input :" + input);
            }

        });

        env.execute("Intsmaze ValueStateFlatMap");
    }
}

