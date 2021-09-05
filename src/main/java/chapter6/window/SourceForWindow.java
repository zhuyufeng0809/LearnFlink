package chapter6.window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-07-23
 * @Description:
 */
public class SourceForWindow implements SourceFunction<Tuple3<String, Integer, String>> {

    private volatile boolean isRunning = true;

    private final long sleepTime;

    private Boolean stopSession = false;

    public SourceForWindow(long sleepTime, Boolean stopSession) {
        this.sleepTime = sleepTime;
        this.stopSession = stopSession;
    }

    public SourceForWindow(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void run(SourceContext<Tuple3<String, Integer, String>> ctx) throws Exception {
        int count = 0;
        while (isRunning) {
            String word = WORDS[count % WORDS.length];
            String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            Tuple3<String, Integer, String> tuple2 = Tuple3.of(word, count, time);
            ctx.collect(tuple2);
            if (stopSession && count == WORDS.length) {
                Thread.sleep(10000);
            } else {
                Thread.sleep(sleepTime);
            }
            count++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static final String[] WORDS = new String[]{
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "intsmaze",
            "java",
            "flink",
            "flink",
            "flink",
            "intsmaze",
            "intsmaze",
            "hadoop",
            "hadoop",
            "spark"
    };
}
