package chapter9;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-10-28
 * @Description:
 */
public class ClickBean {

    public String user;

    public String time;

    public String url;

    public int id;

    public ClickBean() {
    }

    public ClickBean(int id, String user, String url, String time) {
        this.user = user;
        this.url = url;
        this.time = time;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getTime() {
        return LocalDateTime.parse(this.time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.of("+0"));
    }

    public void setTime(String time) {
        this.time = time;
    }


    @Override
    public String toString() {
        return "ClickBean{" +
                "user='" + user + '\'' +
                ", time=" + time +
                ", url='" + url + '\'' +
                '}';
    }
}
