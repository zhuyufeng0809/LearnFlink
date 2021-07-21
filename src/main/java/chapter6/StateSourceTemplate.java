package chapter6;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-12
 * @Description:
 */
public class StateSourceTemplate {

    private abstract static class StateSource<OUT> extends RichParallelSourceFunction<Long>
            implements ListCheckpointed<Long> {}

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);
        String url = "file:///Users/zhuyufeng/IdeaProjects/LearnFlink/src/main/resources";
        StateBackend stateBackend = new RocksDBStateBackend(url, false);
        env.setStateBackend(stateBackend);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));
        env.setParallelism(2);
        DataStream<Long> inputStream = env
                .addSource(new StateSource<Long>() {
                    private List<Long> checkPointedCount = new ArrayList<>();

                    @Override
                    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
                        return checkPointedCount;
                    }

                    @Override
                    public void restoreState(List<Long> state) throws Exception {
                        checkPointedCount = state;
                    }

                    @Override
                    public void run(SourceContext<Long> ctx) throws InterruptedException {

                        while (true) {
                            synchronized (ctx.getCheckpointLock()) {
                                long offset;
                                if (checkPointedCount.size() == 0) {
                                    offset = 0L;
                                } else {
                                    offset = checkPointedCount.get(0);
                                    checkPointedCount.remove(0);
                                }
                                ctx.collect(offset);
                                offset+=1;
                                checkPointedCount.add(0, offset);

                                if (offset == 10) {
                                    int i = 1 / 0;
                                }
                            }

                            Thread.sleep(1000);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(1);

        inputStream.print();

        env.execute("StateSourceTemplate");
    }

}
