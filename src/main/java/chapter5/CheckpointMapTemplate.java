package chapter5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-28
 * @Description:
 */
public class CheckpointMapTemplate {

    public interface CheckpointMap extends MapFunction<Long, String>,
            CheckpointedFunction{}

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        String path = "file:///Users/zhuyufeng/IdeaProjects/LearnFlink/src/main/resources/";
        FsStateBackend stateBackend = new FsStateBackend(path);
        env.setStateBackend(stateBackend);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3,
                Time.of(5, TimeUnit.MINUTES),
                Time.of(10, TimeUnit.SECONDS)
        ));

        DataStream<Long> streamSource = env.addSource(new RichSourceFunction<Long>() {
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
        }).setParallelism(1);

        DataStream<String> mapResult = streamSource
                .map(new CheckpointMap() {
                    private transient ListState<Long> checkpointedState;

                    private final LinkedList<Long> bufferedElements = new LinkedList<>();

                    @Override
                    public String map(Long value) {
                        if (value == 30) {
                            int t = 1 / 0;
                        }

                        int size = bufferedElements.size();
                        if (size >= 10) {
                            for (int i = 0; i < size - 9; i++) {
                                bufferedElements.poll();
                            }
                        }

                        String threadName = Thread.currentThread().getName();
                        bufferedElements.add(value);

                        return " 集合中第一个元素是:" + bufferedElements.getFirst() +
                                " 集合中最后一个元素是:" + bufferedElements.getLast() +
                                " length is :" + bufferedElements.size();
                    }

                    @Override
                    public void snapshotState(FunctionSnapshotContext context) throws Exception {
                        checkpointedState.clear();
                        checkpointedState.addAll(bufferedElements);
                    }

                    @Override
                    public void initializeState(FunctionInitializationContext context) throws Exception {
                        ListStateDescriptor<Long> descriptor =
                                new ListStateDescriptor<>(
                                        "CheckpointedFunctionTemplate-ListState",
                                        TypeInformation.of(new TypeHint<Long>() {
                                        }));
//                        checkpointedState = context.getOperatorStateStore().getUnionListState(descriptor);
                        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
                        if (context.isRestored()) {
                            for (Long element : checkpointedState.get()) {
                                bufferedElements.offer(element);
                            }
                        }
                    }
                });
        mapResult.print("输出结果");
        env.execute("Intsmaze CheckpointedFunctionTemplate");
    }

}