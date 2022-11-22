import chapter6.time.EventBean;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class blog {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(3000);

        env.addSource(new SourceFunction<EventBean>() {
            private volatile boolean isRunning = true;

            private int counter = 0;

            final Date date = new Date();

            final EventBean[] BEANS = new EventBean[]{
                    new EventBean("0.money", date.getTime()),
                    new EventBean("1.money", date.getTime() + 1000),
                    new EventBean("2.money", date.getTime() + 2000),
                    new EventBean("3-money", date.getTime() + 3000),
                    new EventBean("4.late", date.getTime() + 4000),
                    new EventBean("5.money", date.getTime() + 5000),
                    new EventBean("6.money", date.getTime() + 6000),
                    new EventBean("7-money", date.getTime() + 2000),
            };

            @Override
            public void run(SourceContext<EventBean> ctx) throws Exception {
                while (isRunning) {
                    if (counter >= 8) {
                        isRunning = false;
                    } else {
                        EventBean bean = BEANS[counter];
                        ctx.collectWithTimestamp(bean, bean.getTime());
                        System.out.println("send:" + bean.getText() + " " + getTime(bean.getTime()));
                        if (bean.getText().contains("6")) {
                            ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
                            Thread.sleep(8000);
                        } else {
                            Thread.sleep(1000);
                        }
                    }
                    counter++;
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).allowedLateness(Time.seconds(15))
                .reduce((value1, value2) ->
                {
                    value1.setText(value1.getText() + " " + value2.getText());
                    value1.setTimes(value1.getTimes() + " " + value2.getTimes());
                    return value1;
                }).print();

        env.execute();
    }

    static String getTime(long timestamp) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()
                ));
    }
}
