package com.jackie.stream.customPartition;


import org.apache.flink.api.common.functions.Partitioner;
import scala.Function1;

public class MyPartition implements Partitioner<Long> {

    @Override
    public int partition(Long aLong, int i) {
        System.out.println("分区总数"+i);

        if (aLong%2 == 0){
            return 0;
        }else {
            return 1;
        }
    }
}
