package chapter9;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-11-12
 * @Description:
 */
public class MyTableFunction extends TableFunction<Tuple2<String, Integer>> {
    public void eval(String str) {
        if (!str.contains("flink")) {
            String separator = "#";
            for (String s : str.split(separator)) {
                collect(new Tuple2<>(s, s.length()));
            }
        }
    }
}
