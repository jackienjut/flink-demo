package com.jackie.stream.customSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * collection 作为数据源
 */
public class StreamingDemoOfConnect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> text1 = env.addSource(new MyNoParallelSource());

        DataStream<Long> text2 = env.addSource(new MyNoParallelSource());

        DataStream<String> text3 =   text1.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long aLong) throws Exception {
                return "str_"+aLong;
            }
        });

        ConnectedStreams<Long,String> connectStream = text1.connect(text3);


        connectStream.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Long map1(Long value) throws Exception {
                return value;
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });



        String jobName = StreamingDemoOfConnect.class.getSimpleName();
        env.execute(jobName);
    }
}
