package com.jackie.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * collection 作为数据源
 */
public class StreamingFromCollection {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(15);
        data.add(20);
        DataStreamSource<Integer> collectionData = env.fromCollection(data);

        //对map数据就行处理

        SingleOutputStreamOperator<Integer> num = collectionData.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {

                return value + 1;
            }
        });


        //输出
        num.print().setParallelism(1);
        env.execute("stream from collection");


    }
}
