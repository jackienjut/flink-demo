package com.jackie.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class  BatchWordCountJava {


    public static void main(String[] args) throws Exception {
        String inputPath = "d:\\data\\file";
        String outPath = "d:\\data\\result";
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
                .groupBy(0).sum(1);

        counts.writeAsCsv(outPath, "\n", " ");
        env.setParallelism(1);
        env.execute("batch word count");

    }


    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\s+");
            for (String word : tokens) {
                out.collect(new Tuple2(word, 1));
            }
        }
    }
}
