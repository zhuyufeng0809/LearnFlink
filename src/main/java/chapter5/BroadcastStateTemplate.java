package chapter5;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-05
 * @Description:
 */
public class BroadcastStateTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<Integer, String>> ruleStream = env.addSource(new RichSourceFunction<Tuple2<Integer, String>>() {
            private final String[] format = new String[]{"yyyy-MM-dd HH:mm", "yyyy-MM-dd HH",
                    "yyyy-MM-dd", "yyyy-MM", "yyyy"};

            @Override
            public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
                while (true) {
                    for (String rule : format) {
                        ctx.collect(new Tuple2<>(1, rule));
                        Thread.sleep(5000);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        });

        DataStream<Date> mainStream = env.addSource(new RichSourceFunction<Date>() {

            @Override
            public void run(SourceContext<Date> ctx) throws InterruptedException {
                while (true) {
                    ctx.collect(new Date());
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
            }
        });

        final MapStateDescriptor<Integer, String> stateDesc = new MapStateDescriptor<>(
                "broadcast-state", Integer.class, String.class
        );

        BroadcastStream<Tuple2<Integer, String>> broadcastStream = ruleStream.broadcast(stateDesc);

        DataStream<Tuple2<String, String>> result = mainStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<Date, Tuple2<Integer, String>, Tuple2<String, String>>() {
                    private transient MapStateDescriptor<Integer, String> descriptor;

                    @Override
                    public void open(Configuration parameters) {
                        descriptor = new MapStateDescriptor<>(
                                "broadcast-state", Integer.class, String.class
                        );
                    }

                    @Override
                    public void processElement(Date value, ReadOnlyContext ctx,
                                               Collector<Tuple2<String, String>> out) throws Exception {
                        String formatRule = "";
                        for (Map.Entry<Integer, String> entry :
                                //Map.Entry<K,V>，代表Map集合中的每个元素 https://blog.csdn.net/itaem/article/details/8171663
                                ctx.getBroadcastState(descriptor).immutableEntries()) {
                            if (entry.getKey() == 1) {
                                formatRule = entry.getValue();
                            }
                        }
                        if (StringUtils.isNotBlank(formatRule)) {
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String originalDate = format.format(value);
                            format = new SimpleDateFormat(formatRule);
                            String formatDate = format.format(value);
                            out.collect(Tuple2.of("主数据流元素:" + originalDate, "应用规则后的格式：" + formatDate));
                        }
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<Integer, String> value, Context ctx,
                                                        Collector<Tuple2<String, String>> out) throws Exception {
                        BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(descriptor);
                        //这里因为元素的key全都相同，所以后面的元素会覆盖前面元素的值
                        broadcastState.put(value.f0, value.f1);
                        out.collect(new Tuple2<>("广播状态中新增元素", value.toString()));
                    }
                });

        result.print("输出结果");

        env.execute("BroadcastState Template");
    }

}


