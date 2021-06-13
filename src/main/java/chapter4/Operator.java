package chapter4;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.hash.BloomFilter;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-02
 * @Description:
 */
public class Operator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Long> streamSource = env.generateSequence(1, 100);
        DataStream<Long> dataStream = streamSource
                .flatMap(new RichFunctionTemplate())
                .name("intsmaze-flatMap");
        dataStream.print();

        env.execute("RichFunctionTemplate");
    }

}

class RichFunctionTemplate extends RichFlatMapFunction<Long, Long> {

    @Override
    public void open(Configuration parameters) {
        RuntimeContext rc = getRuntimeContext();
        String taskName = rc.getTaskName();
        String subtaskName = rc.getTaskNameWithSubtasks();
        int subtaskIndexOf = rc.getIndexOfThisSubtask();
        int parallel = rc.getNumberOfParallelSubtasks();
        int attemptNum = rc.getAttemptNumber();
        System.out.println("调用open方法：" + taskName + "||" + subtaskName + "||"
        + subtaskIndexOf + "||" + parallel + "||" + attemptNum);
    }

    @Override
    public void flatMap(Long input, Collector<Long> out) throws Exception {
        Thread.sleep(1000);
        out.collect(input);
    }

    @Override
    public void close() {
        System.out.println("调用close方法");
    }
}