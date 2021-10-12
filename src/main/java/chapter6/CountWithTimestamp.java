package chapter6;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-09-07
 * @Description:
 */
public class CountWithTimestamp {

    public String key;

    public int count;

    public long lastModified;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getLastModified() {
        return lastModified;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }
}
