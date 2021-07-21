package chapter6.kafkaSource;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-16
 * @Description:
 */

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaSourceSinkTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties conProperties = new Properties();
        conProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        conProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaSource");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "flink-test", new SimpleStringSchema(), conProperties);

        kafkaConsumer.setStartFromEarliest();

        DataStream<String> streamSource = env.addSource(kafkaConsumer);

        DataStream<String> mapStream = streamSource.map(value -> {
            Thread.sleep(100);
            return String.valueOf(Integer.parseInt(value) + 1);
        });

        mapStream.print();

        env.execute();
    }
}

