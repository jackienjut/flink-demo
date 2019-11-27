package com.jackie;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 * <p>
 * 通过socket模拟产生单词数据
 * flink对数据就行统计计算
 * <p>
 * 需要实现每隔1秒对2秒内的数据就行汇总计算
 */
public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {

        String hostname = "192.168.3.130";
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("not set the port , use the default port");
            port = 9000;
        }

        String delimiter = "\n";

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //链接socket获取数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        SingleOutputStreamOperator windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String splits[] = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))//指定时间窗口大小为2秒，指定时间间隔为1秒
                //  .sum("count"); // 使用sum 或者reduce
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                }) // reduce
                ;
        // ;

        //把数据打印到控制台， 并设置并行度
        windowCount.print().setParallelism(1);
        //这一定要实现，否则程序不执行。
        env.execute("Socket Window Count");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public  WordWithCount(){
        }
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount(word = '"+word + "\'"+
                    ", count =" +count +
                    ")";
        }
    }
}
