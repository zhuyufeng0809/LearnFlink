package chapter6;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-09-06
 * @Description:
 */
public class ProcessTemplate {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Long>> source = env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            final String[] strings = new String[]{"flink", "streaming", "java"};
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                while (isRunning) {
                    Thread.sleep(1000);
                    int index = new Random().nextInt(3);
                    Tuple2<String, Long> tuple2 = new Tuple2<>(strings[index], System.currentTimeMillis());
                    ctx.collect(tuple2);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                return element.f1;
            }
        });

        DataStream<Tuple2<String, Integer>> result = source
                .keyBy("f0")
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Integer>>() {
                    private ValueState<CountWithTimestamp> state;

                    @Override
                    public void open(Configuration parameters) {
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("custom process State", CountWithTimestamp.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("收到元素:" + value);
                        CountWithTimestamp current = state.value();
                        if (current == null) {
                            current = new CountWithTimestamp();
                            current.key = value.f0;
                        }
                        current.count++;
                        current.lastModified = ctx.timestamp();
                        if (current.count % 5 == 0) {
                            ctx.timerService().registerEventTimeTimer(current.lastModified);
                        }
                        state.update(current);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out)
                            throws Exception {
                        CountWithTimestamp result = state.value();
                        out.collect(new Tuple2<>(result.key, result.count));
                    }
                });

        result.print("输出结果");
        env.execute("Process Template");
    }

}

