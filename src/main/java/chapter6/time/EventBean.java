package chapter6.time;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-08-05
 * @Description:
 */
public class EventBean implements Serializable {

    private String text;
    private String times;
    private long time;

    public EventBean(String text, long time) {
        this.text = text;
        this.time = time;
        this.times = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()
                ));
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getTimes() {
        return times;
    }

    public void setTimes(String times) {
        this.times = times;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "EventBean{" +
                "text='" + text + '\'' +
                ", times='" + times + '\'' +
                ", time=" + time +
                '}';
    }
}
