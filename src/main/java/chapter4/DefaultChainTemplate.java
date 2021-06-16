package chapter4;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-15
 * @Description:
 */
public class DefaultChainTemplate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<Tuple2<String, Integer>> inputStream = env.addSource(new RichSourceFunction<Tuple2<String, Integer>>() {
            final int sleep = 6000;

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                String subtaskName = getRuntimeContext().getTaskNameWithSubtasks();
                String info = "source操作所属子任务名称:";
                while (true) {
                    Tuple2<String, Integer> tuple2 = new Tuple2<>("185XXX", 899);
                    ctx.collect(tuple2);
                    System.out.println(info + subtaskName + ",元素:" + tuple2);
                    Thread.sleep(sleep);

                    tuple2 = new Tuple2<>("155XXX", 1199);
                    ctx.collect(tuple2);
                    System.out.println(info + subtaskName + ",元素:" + tuple2);
                    Thread.sleep(sleep);

                    tuple2 = new Tuple2<>("138XXX", 19);
                    ctx.collect(tuple2);
                    System.out.println(info + subtaskName + ",元素:" + tuple2);
                    Thread.sleep(sleep);
                }
            }

            @Override
            public void cancel() {
                System.out.println("调用cancel");
            }
        });

        DataStream<Tuple2<String, Integer>> filter = inputStream.filter(
                new RichFilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) {
                        System.out.println("filter操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                        return true;
                    }
                });

        DataStream<Tuple2<String, Integer>> mapOne = filter.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                System.out.println("map-one操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return value;
            }
        }).startNewChain();

        DataStream<Tuple2<String, Integer>> mapTwo = mapOne.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) {
                System.out.println("map-two操作所属子任务名称:" + getRuntimeContext().getTaskNameWithSubtasks() + ",元素:" + value);
                return value;
            }
        });
        mapTwo.print();

        env.execute("chain");
    }

}
