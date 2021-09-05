package chapter6.window;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-08-12
 * @Description:
 */
public class MyEventTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    public MyEventTimeTrigger() {}

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//        System.out.println("watermark:" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(
//                LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.getCurrentWatermark()), ZoneId.systemDefault()
//                )));
//        System.out.println("maxTimestamp:" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(
//                LocalDateTime.ofInstant(Instant.ofEpochMilli(window.maxTimestamp()), ZoneId.systemDefault()
//                )));
//        System.out.println("maxTimestamp:" + getTime(window.maxTimestamp()));
//        System.out.println("watermark:" + ctx.getCurrentWatermark());
//        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
//            // if the watermark is already past the window fire immediately
//            return TriggerResult.FIRE;
//        } else {
//            ctx.registerEventTimeTimer(window.maxTimestamp());
//            return TriggerResult.CONTINUE;
//        }


//        System.out.println("windowMaxTimestamp:"+getTime(window.maxTimestamp()));
//        System.out.println("elementTimestamp:"+getTime(timestamp));
//        if (window.maxTimestamp() <= timestamp) {
//            return TriggerResult.FIRE;
//        } else {
//            return TriggerResult.CONTINUE;
//        }
        ctx.registerEventTimeTimer(window.maxTimestamp());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        System.out.println("调用onEventTime");
        return time >= window.maxTimestamp() ?
                TriggerResult.FIRE :
                TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static MyEventTimeTrigger create() {
        return new MyEventTimeTrigger();
    }

    String getTime(long timestamp) {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()
                ));
    }
}
