package chapter4;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-01
 * @Description:
 */
public class WordCount {
    public static final String[] WORDS = new String[]{
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
            "com.intsmaze.flink.streaming.window.helloword.WordCountTemplate",
    };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements(WORDS);

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] tokens = value.toLowerCase().split("\\.");
                    System.out.println("mark" + Arrays.toString(tokens));
                    Arrays.stream(tokens).filter(token -> token.length() > 0).
                            map(token -> new Tuple2<>(token, 1)).forEach(out::collect);
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                        .keyBy("f0")
                        .sum(1);

        counts.print("hello dataStream");

        env.execute();
    }

}
