package chapter9;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-15
 * @Description:
 */
public class RateBean {

    public String currency;

    public Timestamp time;

    public int rate;

    public int id;

    public RateBean() {
    }

    public RateBean(int id, String currency, int rate, String time) throws ParseException {
        this.currency = currency;
        this.rate = rate;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
        dateFormat.setLenient(false);
        Date timeDate = dateFormat.parse(time);
        this.time = new Timestamp(timeDate.getTime());
        this.id = id;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public int getRate() {
        return rate;
    }

    public void setRate(int rate) {
        this.rate = rate;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
