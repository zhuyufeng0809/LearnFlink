package chapter6.window;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-09-02
 * @Description:
 */
public class Trade {

    private String name;

    private long amount;

    private String client;

    private long tradeTime;

    public Trade() {
    }

    public Trade(String name, long amount, String client, long tradeTime) {
        this.name = name;
        this.amount = amount;
        this.client = client;
        this.tradeTime = tradeTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public long getTradeTime() {
        return tradeTime;
    }

    public void setTradeTime(long tradeTime) {
        this.tradeTime = tradeTime;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "name='" + name + '\'' +
                ", amount=" + amount +
                ", client='" + client + '\'' +
                ", tradeTime=" + tradeTime +
                '}';
    }
}
