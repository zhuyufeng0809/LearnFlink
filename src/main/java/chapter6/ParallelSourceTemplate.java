package chapter6;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-09
 * @Description:
 */
public class ParallelSourceTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Tuple2<String, Long>> streamSource = env
                .addSource(new RichParallelSourceFunction<Tuple2<String, Long>>() {
                    private long count = 1L;

                    private boolean isRunning = true;

                    private String sourceFlag;

                    @Override
                    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

                        while (isRunning) {
                            count++;
                            if ("DB".equals(sourceFlag)) {
                                ctx.collect(new Tuple2<>("DB", count));
                            } else if ("MQ".equals(sourceFlag)) {
                                ctx.collect(new Tuple2<>("MQ", count));
                            }
                            Thread.sleep(1000);

                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        int parallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        System.out.println("当前任务的并行度为:" + parallelSubtasks);

                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        if (indexOfThisSubtask == 0) {
                            sourceFlag = "DB";
                        } else if (indexOfThisSubtask == 1) {
                            sourceFlag = "MQ";
                        }
                        //super.open(parameters);

                    }

                    @Override
                    public void close() throws Exception {
                        //super.close();
                    }
                });

        streamSource.print();

        env.execute("RichParalleSourceTemplate");
    }
}
