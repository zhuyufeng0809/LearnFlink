package chapter6.time;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-08-04
 * @Description:
 */
public class SourceWithTimestampsWatermarks {

//    private static final long serialVersionUID = 1L;
//
//    private volatile boolean isRunning = true;
//
//    private int counter = 0;
//
//    private long sleepTime;
//
//    public SourceWithTimestampsWatermarks(long sleepTime) {
//        this.sleepTime = sleepTime;
//    }
//
//    @Override
//    public void run(SourceContext<EventBean> ctx) throws Exception {
//
//        Date date = new Date();
//
//        final EventBean[] BEANS = new EventBean[]{
//                new EventBean("0.money", date.getTime()),
//                new EventBean("1.money", date.getTime() + 10000),
//                new EventBean("2.money", date.getTime() + 20000),
//                new EventBean("3-nosleep", date.getTime() + 30000),
//                new EventBean("4.late", date.getTime() + 20000),
//                new EventBean("5.money", date.getTime() + 40000),
//                new EventBean("6.money", date.getTime() + 50000),
//                new EventBean("7-nosleep", date.getTime() + 60000),
//                new EventBean("8-nosleep-late", date.getTime() + 50000),
//                new EventBean("9.late", date.getTime() + 50000),
//                new EventBean("10.money", date.getTime() + 70000),
//                new EventBean("11.money", date.getTime() + 80000),
//                new EventBean("12-nosleep", date.getTime() + 90000),
//                new EventBean("13-late-abandon", date.getTime() + 50000),
//                new EventBean("14.money", date.getTime() + 100000),
//                new EventBean("15.money", date.getTime() + 110000),
//        };
//
//        while (isRunning) {
//            if (counter >= 16) {
//                isRunning = false;
//            } else {
//                EventBean bean = BEANS[counter];
//                ctx.collectWithTimestamp(bean, bean.getTime());
//                if (!bean.getList().get(0).contains("late")) {
//                    ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
//                }
//                if (!bean.getList().get(0).contains("nosleep")) {
//                    Thread.sleep(sleepTime);
//                }
//            }
//            counter++;
//        }
//    }
//
//    @Override
//    public void cancel() {
//        isRunning = false;
//    }

}
