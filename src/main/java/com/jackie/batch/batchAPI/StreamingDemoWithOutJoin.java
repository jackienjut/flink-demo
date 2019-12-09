package com.jackie.batch.batchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * 外连接， 分为 左外连接， 右外连接， 全外连接
 */
public class StreamingDemoWithOutJoin {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> data1 = new ArrayList<Tuple2<Integer, String>>();
        List<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));

        data2.add(new Tuple2<>(1, "sh"));
        data2.add(new Tuple2<>(2, "bj"));
        data2.add(new Tuple2<>(4, "jb"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        //左外连接
        //tuple2中的第二个元素可能为空
        text1.leftOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> data11, Tuple2<Integer, String> data22) throws Exception {
                        if (data22 != null)
                            return new Tuple3<Integer, String, String>(data11.f0, data11.f1, data22.f1);
                        else
                            return new Tuple3<Integer, String, String>(data11.f0, data11.f1, null);

                    }
                }).print();


        //右外连接
        text1.rightOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> data11, Tuple2<Integer, String> data22) throws Exception {
                        if (data11 != null)
                            return new Tuple3<Integer, String, String>(data11.f0, data11.f1, data22.f1);
                        else
                            return new Tuple3<Integer, String, String>(data22.f0, null, data22.f1);

                    }
                }).print();

        //全外连接
        text1.fullOuterJoin(text2).where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> data11, Tuple2<Integer, String> data22) throws Exception {
                        if (data11 != null && data22 != null) {
                            return new Tuple3<Integer, String, String>(data11.f0, data11.f1, data22.f1);
                        } else if (data11 != null && data22 == null) {
                            return new Tuple3<Integer, String, String>(data11.f0, data11.f1, null);
                        } else if (data11 == null && data22 != null) {
                            return new Tuple3<Integer, String, String>(data22.f0, null, data22.f1);
                        } else
                            return null;
                    }
                }).print();
    }
}
