package com.feiyu.test2;

import com.feiyu.gflow.test2.test.Employee;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

public class MF implements MapFunction<ObjectNode, ObjectNode> {

    @Override
    public ObjectNode map(ObjectNode objectNode) throws Exception {

//                    String value = objectNode.get("value").toString();
//                    JSONObject valueJson = JSONObject.parseObject(value);
//                    Timestamp value_time = new Timestamp(System.currentTimeMillis());
//
//                    prepareStatement.setObject(index + 1, valueJson.get(index));


//        Gson gson = new Gson();
//        Employee2 employee = gson.fromJson(string, Employee2.class);
//        Row rowAddr = new Row(6);
//        rowAddr.setField(0, employee.id());
//        rowAddr.setField(1, employee.name());
//        rowAddr.setField(2, employee.password());
//        rowAddr.setField(3, employee.age());
//        rowAddr.setField(4, employee.salary());
//        rowAddr.setField(5, employee.department());
//        return rowAddr;

        String string = objectNode.get("value").toString();
        Gson gson = new Gson();
        Employee2 employee = gson.fromJson(string, Employee2.class);

        Map<Integer, Object> map = new HashMap<Integer,Object>();
        map.put(0, employee.id());
        map.put(1, employee.name());
        map.put(2, employee.password());
        map.put(3, employee.age());
        map.put(4, employee.salary());
        map.put(5, employee.department());
        objectNode.set("value", new TextNode(gson.toJson(map)));

        return objectNode;
    }
}
