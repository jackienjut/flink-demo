package com.jackie.batch.batchAPI;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BathchDemoMapPartition {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        DataSet<String> text = env.fromCollection(data);

    /*    text.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
            // 获取数据库链接， 每次过来一个数据获得一次链接
                return s;
            }
        });
        */

        MapPartitionOperator<String, String> mapPartitionData = text.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> collector) throws Exception {
                // values 保存一个分区的数据
                //获取数据库连接，一个分区的数据获取一次链接
                //优点每个分区获得一次链接
                Iterator<String> its = values.iterator();

                while (its.hasNext()) {
                    String next = its.next();
                    String[] split = next.split("\\W+");
                    for (String word : split) {
                        collector.collect(word);
                    }
                }
            }
        });
        mapPartitionData.print();
    }

}
