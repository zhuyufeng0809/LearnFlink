package chapter4.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-17
 * @Description:
 */
public class CustomTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        final String flag = "subtask name is ";
        DataStream<Trade> inputStream = env.addSource(new RichSourceFunction<Trade>() {

            @Override
            public void run(SourceContext<Trade> ctx) {
                List<Trade> list = new ArrayList<>();
                list.add(new Trade("185XXX", 899, "2018"));//2
                list.add(new Trade("155XXX", 1111, "2019"));//2
                list.add(new Trade("155XXX", 1199, "2019"));//1
                list.add(new Trade("185XXX", 899, "2018"));//2
                list.add(new Trade("138XXX", 19, "2019"));//2
                list.add(new Trade("138XXX", 399, "2020"));//2

                for (Trade trade : list) {
                    ctx.collect(trade);
                }
                String subtaskName = getRuntimeContext().getTaskNameWithSubtasks();
                System.out.println("source operator " + flag + subtaskName);
            }

            @Override
            public void cancel() {
                System.out.println("调用cancel方法");
            }
        });

        inputStream.map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                RuntimeContext context = getRuntimeContext();
                String subtaskName = context.getTaskNameWithSubtasks();
                int subtaskIndexOf = context.getIndexOfThisSubtask();
                System.out.println(value + " first map operator " + flag + subtaskName + " index:" + subtaskIndexOf);
                return value;
            }
        }).global().map(new RichMapFunction<Trade, Trade>() {
            @Override
            public Trade map(Trade value) {
                RuntimeContext context = getRuntimeContext();
                String subtaskName = context.getTaskNameWithSubtasks();
                int subtaskIndexOf = context.getIndexOfThisSubtask();
                System.out.println(value + " second map operator " + flag + subtaskName + " index:" + subtaskIndexOf);
                return value;
            }
        });

        env.execute("Physical partitioning");
    }

}
