package chapter6.window;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-09-02
 * @Description:
 */
public class ClickBean {
    public String user;

    public long visitTime;

    public String url;

    public int id;

    public ClickBean() {
    }

    public ClickBean(int id,String user, String url, long visitTime) {
        this.user = user;
        this.visitTime = visitTime;
        this.url = url;
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public long getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(long visitTime) {
        this.visitTime = visitTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "ClickBean{" +
                "user='" + user + '\'' +
                ", visitTime=" + visitTime +
                ", url='" + url + '\'' +
                ", id=" + id +
                '}';
    }
}
