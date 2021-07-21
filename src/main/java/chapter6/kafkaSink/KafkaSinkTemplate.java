package chapter6.kafkaSink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-21
 * @Description:
 */
public class KafkaSinkTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> streamSource = env.fromElements("202107211922");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "localhost:9092",
                "flink-test",
                new SimpleStringSchema());

        streamSource.addSink(kafkaProducer);

        env.execute("KafkaSink");
    }
}
