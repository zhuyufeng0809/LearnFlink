package chapter3;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-01
 * @Description:
 */
public class lambda {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, String>> dataStream = env.fromElements(Tuple2.of(1, "1"), Tuple2.of(2, "2"));

        DataStream<Tuple2<Integer, String>> transStream1 = dataStream.map(i -> i).returns(Types.TUPLE(
                Types.INT,Types.STRING
        ));
        DataStream<Tuple2<Integer, String>> transStream2 = transStream1.map(i -> i).returns(Types.TUPLE(
                Types.INT,Types.STRING
        ));

        transStream2.print();

        env.execute();
    }
}
