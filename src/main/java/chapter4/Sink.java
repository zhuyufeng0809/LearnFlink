package chapter4;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-02
 * @Description:
 */
public class Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        DataStream<Integer> dataStream = env.fromElements(1,2,3);

        dataStream.print("mark");

        env.execute("stdOut DataStream");
    }
}
