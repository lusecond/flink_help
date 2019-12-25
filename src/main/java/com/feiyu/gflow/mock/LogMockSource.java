package com.feiyu.gflow.mock;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class LogMockSource extends RichSourceFunction<String> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(LogMockSource.class);

    private volatile boolean isRunning = true;

    // 尝试模拟生成日志
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        log.info("run");
        while (isRunning){
            ctx.collect(activity());
            Thread.sleep(1);
        }
    }

    // 生成激活日志
    public String activity() {
        return "";
    }

    @Override
    public void cancel() {
        log.info("cancel");
        isRunning = false;
    }
}