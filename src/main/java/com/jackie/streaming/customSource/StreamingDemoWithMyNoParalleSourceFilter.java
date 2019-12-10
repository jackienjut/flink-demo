package com.jackie.streaming.customSource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * collection 作为数据源
 */
public class StreamingDemoWithMyNoParalleSourceFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new MyNoParallelSource());

        //对map数据就行处理
        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {

                for (int i = 2; i < aLong; i++) {
                    if (aLong % i == 0)
                        return false;

                    break;
                }
                return true;
            }
        });

        //每隔两秒中处理一次数据
        DataStream sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //输出
        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyNoParalleSourceFilter.class.getSimpleName();
        env.execute(jobName);
    }
}
