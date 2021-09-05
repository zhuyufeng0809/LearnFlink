package chapter6.time;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-08-16
 * @Description:
 */
public class EventTimeWaterMarks implements AssignerWithPeriodicWatermarks<EventBean> {

    //加锁
    EventBean eventBean;

    @Override
    public long extractTimestamp(EventBean element, long previousElementTimestamp) {
        this.eventBean = element;
        return eventBean.getTime();
    }

    @Override
    public Watermark getCurrentWatermark() {
        //long watermark = System.currentTimeMillis();
        return new Watermark(this.eventBean.getTime() - 5000);
    }
}

