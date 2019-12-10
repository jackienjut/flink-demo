package com.jackie.streaming.customPartition;

import com.jackie.streaming.customSource.MyNoParallelSource;
import com.jackie.streaming.customSource.StreamingDemoWithMyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 根据数字的奇偶性来进行分区
 */
public class StreamingDemoWithMyPartition {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Long> text = env.addSource(new MyNoParallelSource());
        env.setParallelism(2);

        //对数据进行转换， 把long数据转tuple类型
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long aLong) throws Exception {
                return new Tuple1<>(aLong);
            }
        });

        DataStream<Tuple1<Long>> partitionData =   tupleData.partitionCustom(new MyPartition(),0);

        SingleOutputStreamOperator<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> longTuple1) throws Exception {
                System.out.println("获取当前线程id ：" + Thread.currentThread().getId());
                return longTuple1.getField(0);
            }
        });



        result.print().setParallelism(1);

        String jobName = StreamingDemoWithMyNoParalleSource.class.getSimpleName();
        env.execute(jobName);
    }
}
