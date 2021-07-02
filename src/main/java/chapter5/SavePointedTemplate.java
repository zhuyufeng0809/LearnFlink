package chapter5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.LinkedList;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-02
 * @Description:
 */
public class SavePointedTemplate {

    private interface ListCheckpointMap extends MapFunction<Long, String>,
            ListCheckpointed<Long> {}

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(20000);

        String path = "file:///Users/zhuyufeng/IdeaProjects/LearnFlink/src/main/resources";
        StateBackend stateBackend = new RocksDBStateBackend(path);
        env.setStateBackend(stateBackend);

        DataStream<Long> sourceStream = env.addSource(new RichSourceFunction<Long>() {
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

        sourceStream.map(new ListCheckpointMap() {
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

                int size = bufferedElements.size();
                if (size >= 10) {
                    for (int i = 0; i < size - 9; i++) {
                        bufferedElements.remove(0);
                    }
                }
                bufferedElements.add(value);

                return "集合中第一个元素是:" + bufferedElements.get(0) +
                        " 集合中最后一个元素是:" + bufferedElements.get(bufferedElements.size() - 1) +
                        " length is :" + bufferedElements.size();
            }
        }).uid("map-id").print();

        env.execute("Intsmaze SavePointedTemplate");
    }
}