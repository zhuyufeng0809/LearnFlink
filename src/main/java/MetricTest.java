import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-08-10
 * @Description:
 */
public class MetricTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new RichSourceFunction<Integer>() {
            private transient Counter counter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.counter = getRuntimeContext().
                        getMetricGroup().
                        addGroup("MyMetricsKey", "MyMetricsValue").counter("myTestCounter");
            }

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                while (true) {
                    ctx.collect(1);
                    Thread.sleep(2000);
                    this.counter.inc();
                }
            }

            @Override
            public void cancel() {

            }
        }).name("mySourceOperator").print();

        env.execute("MetricTest");
    }
}
