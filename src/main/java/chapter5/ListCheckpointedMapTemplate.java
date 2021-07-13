package chapter5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-28
 * @Description:
 */
public class ListCheckpointedMapTemplate {

    private interface ListCheckpointMap extends MapFunction<Long, String>,
            ListCheckpointed<Long>{}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        String path = "file:///Users/zhuyufeng/IdeaProjects/LearnFlink/src/main/resources/";
        StateBackend stateBackend = new FsStateBackend(path);
        env.setStateBackend(stateBackend);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));

        env.addSource(new RichSourceFunction<Long>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void open(Configuration parameters) throws Exception {
                Thread.sleep(10000);
            }

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                long offset = 0L;
                while (true) {
                    ctx.collect(offset);
                    offset += 1;
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
            }
        }).setParallelism(1)
                .map(new ListCheckpointMap() {
                    private List<Long> bufferedElements = new LinkedList<>();

                    @Override
                    public List<Long> snapshotState(long checkpointId, long timestamp) {
                        return bufferedElements;
                    }

                    @Override
                    public void restoreState(List<Long> state) {
                        bufferedElements = state;
                    }

                    @Override
                    public String map(Long value) {
                        if (value == 30) {
                            int i = 1 / 0;
                        }

                        int size = bufferedElements.size();
                        if (size >= 10) {
                            for (int i = 0; i < size - 9; i++) {
                                Long poll = bufferedElements.remove(0);
                            }
                        }
                        bufferedElements.add(value);

                        return "集合中第一个元素是:" + bufferedElements.get(0) +
                                " 集合中最后一个元素是:" + bufferedElements.get(bufferedElements.size() - 1) +
                                " length is :" + bufferedElements.size();
                    }
                }).print("输出结果");

        env.execute("Intsmaze CheckpointedFunctionTemplate");
    }

}
