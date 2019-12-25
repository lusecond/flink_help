package com.feiyu.test1;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class MF implements MapFunction<String, Row> {

    @Override
    public Row map(String string) throws Exception {
        Gson gson = new Gson();
        Employee2 employee = gson.fromJson(string, Employee2.class);
        Row rowAddr = new Row(6);
        rowAddr.setField(0, employee.id());
        rowAddr.setField(1, employee.name());
        rowAddr.setField(2, employee.password());
        rowAddr.setField(3, employee.age());
        rowAddr.setField(4, employee.salary());
        rowAddr.setField(5, employee.department());
        return rowAddr;
    }
}
