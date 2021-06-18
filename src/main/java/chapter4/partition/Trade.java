package chapter4.partition;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-17
 * @Description:
 */
public class Trade {
    private String cardNum;

    private int trade;

    private String time;

    public Trade() {
    }

    public Trade(String cardNum, int trade, String time) {
        super();
        this.cardNum = cardNum;
        this.trade = trade;
        this.time = time;
    }

    public String getCardNum() {
        return cardNum;
    }

    public void setCardNum(String cardNum) {
        this.cardNum = cardNum;
    }

    public int getTrade() {
        return trade;
    }

    public void setTrade(int trade) {
        this.trade = trade;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "元素(" + cardNum + "," + trade + "," + time + ")";
    }
}
