package chapter6;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-09-06
 * @Description:
 */
public class SideOutputTemplate {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final OutputTag<String> REJECTED_WORDS_TAG = new OutputTag<String>("rejected") {
        };

        final String[] DATA = new String[]{
                "In addition to the main stream that results from DataStream operations",
                "When using side outputs",
                "We recommend you",
                "you first need to define an OutputTag that will be used to identify a side output stream"
        };
        DataStream<String> inputStream = env.fromElements(DATA);

        SingleOutputStreamOperator<String> processStream = inputStream
                .process(new ProcessFunction<String, String>() {
                             @Override
                             public void processElement(String value, Context ctx, Collector<String> out) {
                                 if (value.length() < 20) {
                                     ctx.output(REJECTED_WORDS_TAG, value);
                                 } else {
                                     out.collect(value);
                                 }
                             }
                         }
                );

        processStream.print("mainStream");
        processStream.getSideOutput(REJECTED_WORDS_TAG).print("rejectedStream");

        env.execute("test Side Output");
    }
}
