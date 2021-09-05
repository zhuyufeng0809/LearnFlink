package chapter6.window;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-09-02
 * @Description:
 */
public class TumblingWindowJoinTemplate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final Date date = new Date();

        List<ClickBean> clicksData = new ArrayList<>();
        clicksData.add(new ClickBean(1, "张三", "./intsmaze", date.getTime() + 1000));
        clicksData.add(new ClickBean(2, "李四", "./flink", date.getTime() + 2000));
        clicksData.add(new ClickBean(3, "张三", "./stream", date.getTime() + 3000));
        clicksData.add(new ClickBean(4, "李四", "./intsmaze", date.getTime() + 4000));
        clicksData.add(new ClickBean(5, "王五", "./flink", date.getTime() + 5000));
        DataStream<ClickBean> clickStream = env.fromCollection(clicksData)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ClickBean>() {
                    @Override
                    public long extractTimestamp(ClickBean element, long previousElementTimestamp) {
                        return element.getVisitTime();
                    }

                    @Override
                    public Watermark checkAndGetNextWatermark(ClickBean lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.getVisitTime() - 3000);
                    }
                });

        List<Trade> tradeData = new ArrayList<>();
        tradeData.add(new Trade("张三", 38, "安卓手机", date.getTime() + 1000));
        tradeData.add(new Trade("王五", 45, "苹果手机", date.getTime() + 2000));
        tradeData.add(new Trade("张三", 18, "台式机", date.getTime() + 3000));
        tradeData.add(new Trade("王五", 23, "笔记本", date.getTime() + 4000));
        DataStream<Trade> tradeStream = env.fromCollection(tradeData)
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Trade>() {
                    @Override
                    public long extractTimestamp(Trade element, long previousElementTimestamp) {
                        return element.getTradeTime();
                    }

                    @Override
                    public Watermark checkAndGetNextWatermark(Trade lastElement, long extractedTimestamp) {
                        return new Watermark(lastElement.getTradeTime() - 3000);
                    }
                });

        KeyedStream<ClickBean, String> clickKeyedStream = clickStream
                .keyBy((KeySelector<ClickBean, String>) ClickBean::getUser);
        KeyedStream<Trade, String> tradeKeyedStream = tradeStream
                .keyBy((KeySelector<Trade, String>) Trade::getName);

        clickKeyedStream.intervalJoin(tradeKeyedStream)
                .between(Time.minutes(-30), Time.minutes(20))
                .process(new ProcessJoinFunction<ClickBean, Trade, String>() {
                    @Override
                    public void processElement(ClickBean left, Trade right,
                                               Context ctx, Collector<String> out) {
                        out.collect(left.toString() + " : " + right.toString());
                    }
                }).print();

        env.execute("WindowIntervalJoinTumblingTemplate");
    }

}
