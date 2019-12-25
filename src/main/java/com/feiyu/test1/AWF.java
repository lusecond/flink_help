package com.feiyu.test1;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AWF implements AllWindowFunction<Row, List<Row>, TimeWindow> {

    private static final Logger log = LoggerFactory.getLogger(AWF.class);

    @Override
    public void apply(TimeWindow window, Iterable<Row> values, Collector<List<Row>> out) throws Exception {
        ArrayList<Row> model = Lists.newArrayList(values);
        if (model.size() > 0) {
            log.info("10 秒内收集到 employee 的数据条数是：" + model.size());
//            System.out.println("10 秒内收集到 employee 的数据条数是：" + model.size());
//            System.out.println(model);
            out.collect(model);
            log.info("collect 执行完毕");
        }
    }
}
