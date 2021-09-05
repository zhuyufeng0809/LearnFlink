package chapter6.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;

import java.util.concurrent.TimeUnit;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-08-12
 * @Description:
 */
public class WindowsDesc {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<Tuple2<String, Integer>> source = env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window = source
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                .reduce((value1, value2) -> null);

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>) window.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator = transform.getOperator();

        if (operator instanceof EvictingWindowOperator) {
            EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator =
                    (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
            System.out.println(winOperator.getEvictor());
            System.out.println(winOperator.getTrigger());
            System.out.println(winOperator.getWindowAssigner());
            System.out.println(winOperator.getStateDescriptor());
        } else {
            WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                    (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
            System.out.println(winOperator.getTrigger());
            System.out.println(winOperator.getWindowAssigner());
            System.out.println(winOperator.getStateDescriptor());
        }
    }
}
