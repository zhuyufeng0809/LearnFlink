package chapter4;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2021-06-02
 * @Description:
 */
public class Source {

    static final String filePath = "/Users/zhuyufeng/IdeaProjects/LearnFlink/src/main/resources/TextFileSource.txt";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TextInputFormat textInputFormat = new TextInputFormat(new Path(filePath));

        DataStream<String> dataStream = env.readFile(textInputFormat,
                filePath,
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                3000,
                BasicTypeInfo.STRING_TYPE_INFO
                );

        dataStream.print();

        env.execute("fileSource DataStream");
    }
}
