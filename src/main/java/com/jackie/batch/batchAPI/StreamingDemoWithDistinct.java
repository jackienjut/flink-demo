package com.jackie.batch.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据数字的奇偶性来进行分区
 */
public class StreamingDemoWithDistinct {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");
        DataSource<String> text = env.fromCollection(data);

        FlatMapOperator<String, String> flatMapData = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.toLowerCase().split("\\W+");
                for (String word : splits) {
                    System.out.print("words:"+word);
                    collector.collect(word);
                }
            }
        });

        flatMapData.distinct().print();
    }
}
