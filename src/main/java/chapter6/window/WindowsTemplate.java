package chapter6.window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-26
 * @Description:
 */
public class WindowsTemplate {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Integer, String>> streamSource = env.addSource(
                new SourceFunction<Tuple3<String, Integer, String>>() {
                    public final String[] WORDS = new String[]{
                            "intsmaze",
                            "intsmaze",
                            "intsmaze",
                            "intsmaze",
                            "intsmaze",
                            "java",
                            "flink",
                            "flink",
                            "flink",
                            "intsmaze",
                            "intsmaze",
                            "hadoop",
                            "hadoop",
                            "spark"
                    };

                    @Override
                    public void run(SourceContext<Tuple3<String, Integer, String>> ctx) throws Exception {
                        int count = 0;
                        while (true) {
                            String word = WORDS[count % WORDS.length];
                            String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                            Tuple3<String, Integer, String> tuple2 = Tuple3.of(word, count, time);
                            System.out.println("send:" + tuple2);
                            ctx.collect(tuple2);
                            Thread.sleep(1000);
                            count++;
                        }
                    }

                    @Override
                    public void cancel() {
                    }
                }
        );

        streamSource.keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple3<String, Integer, String>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple3<String, Integer, String>> input, Collector<String> out) {
                        StringBuilder result = new StringBuilder();
                        for (Tuple3<String, Integer, String> in : input) {
                            result.append(in.toString());
                        }

                        out.collect(result.toString());
                    }
                }).print();

        env.execute("ProcessWindowTemplate");

    }
}
