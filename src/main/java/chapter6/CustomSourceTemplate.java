package chapter6;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-07
 * @Description:
 */
public class CustomSourceTemplate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStream<Long> inputStream = env.addSource(new SourceFunction<Long>() {
            private static final long serialVersionUID = 1L;

            private volatile boolean isRunning = true;
            private long counter = 0;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                while (isRunning) {
                    ctx.collect(counter);
                    System.out.println("send data :" + counter);
                    counter++;
                    long sleepTime = 100;
                    Thread.sleep(sleepTime);
                }
            }

            @Override
            public void cancel() {
                isRunning = true;
            }
        });

        DataStream<Long> inputStream1 = inputStream
                .map((Long values) ->
                    values + System.currentTimeMillis());
        inputStream1.print();

        env.execute("Intsmaze Custom Source");
    }
}

