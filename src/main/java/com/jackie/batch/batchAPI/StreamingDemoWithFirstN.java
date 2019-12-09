package com.jackie.batch.batchAPI;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据数字的奇偶性来进行分区
 */
public class StreamingDemoWithFirstN {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> data1 = new ArrayList<Tuple2<Integer, String>>();
        List<Tuple2<Integer, String>> data2 = new ArrayList<Tuple2<Integer, String>>();

        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(1, "zs1"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(2, "ls1"));
        data1.add(new Tuple2<>(3, "ww"));
        data1.add(new Tuple2<>(3, "ww1"));

        data1.add(new Tuple2<>(1, "sh"));
        data1.add(new Tuple2<>(1, "sh1"));
        data1.add(new Tuple2<>(2, "bj"));
        data1.add(new Tuple2<>(2, "bj1"));
        data1.add(new Tuple2<>(3, "jb"));
        data1.add(new Tuple2<>(3, "jb1"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);

       /* text1.first(2).print();

        text1.groupBy(0).first(2).print();

        //分组，排序
        text1.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
*/
        //不分组吗全局排序，前3个元素
        text1.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).print();

      /*  text1.cross(text2).with(new CrossFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
            @Override
            public Tuple4<Integer, String, Integer, String> cross(Tuple2<Integer, String> data11, Tuple2<Integer, String> data22) throws Exception {
                return new Tuple4<>(data11.f0, data11.f1, data22.f0, data22.f1);
            }
        }).print();*/

    }
}
