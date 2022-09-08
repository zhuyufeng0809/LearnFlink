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
public class Order {

    public String currency;

    public Timestamp time;

    public int amount;

    public int id;

    public Order() {
    }

    public Order(int id, String currency, String time, int amount) throws ParseException {
        this.currency = currency;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
        dateFormat.setLenient(false);
        Date timeDate = dateFormat.parse(time);
        this.time = new Timestamp(timeDate.getTime());
        this.amount = amount;
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

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
