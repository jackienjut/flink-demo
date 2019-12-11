package com.jackie.batch.batchAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据数字的奇偶性来进行分区
 */
public class StreamingDemoWithJoin {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> data1 = new ArrayList<Tuple2<Integer, String>>();
        List<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));

        data2.add(new Tuple2<>(1, "sh"));
        data2.add(new Tuple2<>(2, "bj"));
        data2.add(new Tuple2<>(3, "jb"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);
/*

        text1.join(text2).where(0)//text1 中的数据
                .equalTo(0) // text2中的数据
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> data1, Tuple2<Integer, String> data2) throws Exception {
                        return new Tuple3<Integer, String, String>(data1.f0, data1.f1, data2.f1);
                    }
                }).print();
*/

        text1.join(text2).where(0)//text1 中的数据
                .equalTo(0) // text2中的数据
                .map(new MapFunction<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>, Tuple3<Integer, String, String>>() {

                    @Override
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> tuple2Tuple2Tuple2) throws Exception {
                        return new Tuple3<Integer, String, String>(tuple2Tuple2Tuple2.f0.f0, tuple2Tuple2Tuple2.f0.f1, tuple2Tuple2Tuple2.f1.f1);
                    }
                }).print();
    }
}
