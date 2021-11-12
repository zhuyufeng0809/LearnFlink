package chapter9;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-12
 * @Description:
 */
public class MyScalarFunction extends ScalarFunction {

    public String eval(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(dateStr);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(date);
    }

    public String eval(String dateStr, int num) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        Date date = sdf.parse(dateStr);
        calendar.setTime(date);
        int day = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, day - num);
        return sdf.format(calendar.getTime());
    }

    private int sleepTime;

    @Override
    public void open(FunctionContext context) {
        sleepTime = Integer.parseInt(context.getJobParameter("sleepTime", "1"));
    }
}
