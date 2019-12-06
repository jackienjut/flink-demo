package com.jackie.stream.customSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * collection 作为数据源
 */
public class StreamingDemoOfUnion {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> text1 = env.addSource(new MyNoParallelSource());

        DataStream<Long> text2 = env.addSource(new MyNoParallelSource());

        DataStream<Long> text = text1.union(text2);

        //对map数据就行处理
        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value;
            }
        });

        //每隔两秒中处理一次数据
        DataStream sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //输出
        sum.print().setParallelism(1);

        String jobName = StreamingDemoOfUnion.class.getSimpleName();
        env.execute(jobName);
    }
}
