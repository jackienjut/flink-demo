package com.jackie.stream.customSource;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class MyRichParallelSource extends RichSourceFunction<Long> {
    private long count = 0L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

        while (isRunning) {
            ctx.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
