package com.jackie;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * window 增量聚合
 */
public class SocketDemoIncrAgg {

    public static void main(String[] args) throws Exception {

        String hostname = "192.168.3.130";
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("not set the port , use the default port -- java");
            port = 9000;
        }

        String delimiter = "\n";

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //链接socket获取数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<Tuple2<Integer, Integer>> intText = text.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String s) throws Exception {
                return new Tuple2<Integer, Integer>(1, Integer.parseInt(s));
            }
        });

        intText.keyBy(0).timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
                        System.out.println("执行reduce:" + v1 + "  " + v2);
                        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                }).print();
        env.execute("Socket Window Count");
    }

    public static class WordWithCount {
        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount(word = '" + word + "\'" +
                    ", count =" + count +
                    ")";
        }
    }
}
