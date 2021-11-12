package chapter9;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-12
 * @Description:
 */
public class MyAccumulator extends AggregateFunction<Double, AccumulatorBean> {

    @Override
    public AccumulatorBean createAccumulator() {
        return new AccumulatorBean();
    }

    @Override
    public Double getValue(AccumulatorBean acc) {
        return acc.totalPrice / acc.totalNum;
    }

    public void accumulate(AccumulatorBean acc, double price, int num) {
        acc.totalPrice += price * num;
        acc.totalNum += num;
    }

    public void retract(AccumulatorBean acc, long iValue, int iWeight) {
        acc.totalPrice -= iValue * iWeight;
        acc.totalNum -= iWeight;
    }

    public void merge(AccumulatorBean acc, Iterable<AccumulatorBean> it) {
        Iterator<AccumulatorBean> iter = it.iterator();
        while (iter.hasNext()) {
            AccumulatorBean a = iter.next();
            acc.totalNum += a.totalNum;
            acc.totalPrice += a.totalPrice;
        }
    }

    public void resetAccumulator(AccumulatorBean acc) {
        acc.totalNum = 0;
        acc.totalPrice = 0L;
    }
}
