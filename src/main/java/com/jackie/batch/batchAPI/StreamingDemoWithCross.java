package com.jackie.batch.batchAPI;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据数字的奇偶性来进行分区
 */
public class StreamingDemoWithCross {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> data1 = new ArrayList<Tuple2<Integer, String>>();
        List<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));

        data2.add(new Tuple2<>(4, "sh"));
        data2.add(new Tuple2<>(5, "bj"));
        data2.add(new Tuple2<>(6, "jb"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        text1.cross(text2).with(new CrossFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
            @Override
            public Tuple4<Integer, String, Integer, String> cross(Tuple2<Integer, String> data11, Tuple2<Integer, String> data22) throws Exception {
                return new Tuple4<>(data11.f0, data11.f1, data22.f0, data22.f1);
            }
        }).print();

    }
}
