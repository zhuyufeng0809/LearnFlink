package chapter4;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-18
 * @Description:
 */
public class DistributedCacheTemplate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String cacheUrl = "/Users/zhuyufeng/IdeaProjects/LearnFlink/src/main/resources/TextFileSource.txt";
        env.setParallelism(3);
        env.registerCachedFile(cacheUrl, "localFile");

        DataStream<Long> input = env.generateSequence(1, 20);

        input.map(new RichMapFunction<Long, String>() {
            private String cacheStr;

            @Override
            public void open(Configuration config) {
                File myFile = getRuntimeContext().getDistributedCache().getFile("localFile");
                cacheStr = readFile(myFile);
            }

            @Override
            public String map(Long value) throws Exception {
                Thread.sleep(6000);
                return StringUtils.join(value, "---", cacheStr);
            }

            public String readFile(File myFile) {
                System.out.println("fuck fuck fuck" + myFile.getPath());
                BufferedReader reader = null;
                StringBuilder sbf = new StringBuilder();
                try {
                    reader = new BufferedReader(new FileReader(myFile));
                    String tempStr;
                    while ((tempStr = reader.readLine()) != null) {
                        sbf.append(tempStr);
                    }
                    reader.close();
                    return sbf.toString();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
                return sbf.toString();
            }
        }).print();

        env.execute();
    }
}

