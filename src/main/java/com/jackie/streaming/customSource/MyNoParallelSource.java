package com.jackie.streaming.customSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 实现并行度为1的source
 *
 * source function 需要指定数据类型， 如果不指定，代码运行的时候会报错
 */
public class MyNoParallelSource implements SourceFunction<Long> {

    private long count = 0L;
    private boolean isRunning = true;

    /**
     * 主要方法，启动一个source
     * 大部分情况，都需要再run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {

        while (isRunning) {
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }

    }

    /**
     * 取消cancal时候调用的方法
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}
