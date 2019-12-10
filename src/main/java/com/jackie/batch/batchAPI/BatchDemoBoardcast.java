package com.jackie.batch.batchAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import scala.Int;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 广播变量。boardcast
 * <p>
 * flink 会从数据中获取用户的姓名
 * <p>
 * 最总flink吧用户的姓名和年龄打印出来
 * <p>
 * 分析：
 * 所以在中间的map处理的时候提取用户的姓名和年龄信息
 * <p>
 * 建议把用户的关系数据集合使用广播变量处理
 */
public class BatchDemoBoardcast {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //准备广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<Tuple2<String, Integer>>();
        broadData.add(new Tuple2<>("zs", 11));
        broadData.add(new Tuple2<>("ls", 21));
        broadData.add(new Tuple2<>("ww", 31));
        broadData.add(new Tuple2<>("zl", 41));

        DataSet<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        //处理需要广播的数据
        DataSet<Map<String, Integer>> toBroadcast = tupleData.map(new MapFunction<Tuple2<String, Integer>, Map<String, Integer>>() {

            @Override
            public Map<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                Map<String, Integer> map = new HashMap<>();
                map.put(value.f0, value.f1);
                return map;
            }
        });

        DataSet<String> data = env.fromElements("zs", "li", "ww", "zl");


        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            //这个方法只会执行一次， 可以在这里初始化的功能
            //在open 方法中获取广播变量数据
            List<Map<String, Integer>> broadMap = new ArrayList<>();
            Map<String, Integer> boardMapD = new HashMap<String, Integer>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //获取广播数据
                this.broadMap = getRuntimeContext().getBroadcastVariable("broadcastMapName");
                for (Map map : broadMap) {
                    boardMapD.putAll(map);
                }
            }

            @Override
            public String map(String s) throws Exception {
                Integer age = boardMapD.get(s);
                return s + "age: "+age;
            }
        }).withBroadcastSet(toBroadcast, "broadcastMapName");//执行广播数据的操作
        result.print();
    }
}
