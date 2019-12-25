package com.feiyu.gflow.test2.test;

import com.esotericsoftware.kryo.Serializer;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.*;

public class KafkaSinkMysql {

    public static void main(String[] args) throws Exception {
//        Gson gson = new Gson();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().enableForceKryo();
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092");
        props.put("group.id", "test");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");//earliest

        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

//        SingleOutputStreamOperator<Employee> empStream = env.addSource(new FlinkKafkaConsumer011<String>(
//                "demo",   //这个 kafka topic 需和生产消息的 topic 一致
//                new SimpleStringSchema(),
//                props)).setParallelism(1)
//                .map(new MapFunction<String, Employee>() {
//                    @Override
//                    public Employee map(String string) throws Exception {
//                        Gson gson = new Gson();
//                        return gson.fromJson(string, Employee.class);
//                    }
//                }); //，解析字符串成JSON对象
//
//        //开个一分钟的窗口去聚合
//        empStream.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Employee, List<Employee>, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow window, Iterable<Employee> values, Collector<List<Employee>> out) throws Exception {
//                ArrayList<Employee> employees = Lists.newArrayList(values);
//                if (employees.size() > 0) {
//                    System.out.println("1 分钟内收集到 employee 的数据条数是：" + employees.size());
//                    out.collect(employees);
//                }
//            }
//        }).addSink(new SinkToMySQL());


//        env.addSource(new FlinkKafkaConsumer011<String>(
//                "demo",   //这个 kafka topic 需和生产消息的 topic 一致
//                new SimpleStringSchema(), props)).setParallelism(1)
//                .map(new MapFunction<String, Msg>() {
//                    @Override
//                    public Msg map(String string) throws Exception {
//                        Gson gson = new Gson();
//                        Map<String, String> map = new HashMap<>();
//                        map.put("msg", string);//(new StringSerial(string)).getString()
////                        map.put("msg", "uibo");
//                        map.put("msgType", "Employee");
//                        String msg = gson.toJson(map);
//                        return gson.fromJson(msg, Msg.class);
////                        return (new Msg(string));
//                    }
//                })
////                .print();
//                .timeWindowAll(Time.minutes(1))
//                .apply(new AllWindowFunction<Msg, List<Msg>, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow window, Iterable<Msg> values, Collector<List<Msg>> out) throws Exception {
//                        ArrayList<Msg> model = Lists.newArrayList(values);
//                        if (model.size() > 0) {
//                            System.out.println("1 分钟内收集到 employee 的数据条数是：" + model.size());
//                            out.collect(model);
//                        }
//                    }
//                })
////                .print();
//                .addSink(new SinkToMySQLMsg());


//        env.addSource(new FlinkKafkaConsumer011<String>(
//                "demo",   //这个 kafka topic 需和生产消息的 topic 一致
//                new SimpleStringSchema(),
//                props)).setParallelism(1)
//                .map(new MapFunction<String, Row>() {
//                    @Override
//                    public Row map(String string) throws Exception {
//                        Gson gson = new Gson();
//                        Employee2 employee = gson.fromJson(string, Employee2.class);
//                        Row rowAddr = new Row(6);
//                        rowAddr.setField(0, employee.id());
//                        rowAddr.setField(1, employee.name());
//                        rowAddr.setField(2, employee.password());
//                        rowAddr.setField(3, employee.age());
//                        rowAddr.setField(4, employee.salary());
//                        rowAddr.setField(5, employee.department());
//                        return rowAddr;
//                    }
//                })
////                .print();
//                .timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Row, List<Row>, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow window, Iterable<Row> values, Collector<List<Row>> out) throws Exception {
//                ArrayList<Row> model = Lists.newArrayList(values);
//                if (model.size() > 0) {
//                    System.out.println("1 分钟内收集到 employee 的数据条数是：" + model.size());
//                    out.collect(model);
//                }
//            }
//        })
////                .print();
//                .addSink(new MySQLTwoPhaseCommitSink(
//                        "jdbc:mysql://10.250.0.38:3306/xy_data_20027?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true" ,
//                        "com.mysql.jdbc.Driver",
//                        "public",
//                        "public",
//                        "insert into employee(id, name, password, age, salary, department) values(?, ?, ?, ?, ?, ?);",
//                        (new int[]{4,12,12,4,4,12})));

        env.addSource(new FlinkKafkaConsumer011<String>(
                "demo",   //这个 kafka topic 需和生产消息的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(new MapFunction<String, Employee2>() {
                    @Override
                    public Employee2 map(String string) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(string, Employee2.class);
                    }
                })
//                .print();
                .timeWindowAll(Time.seconds(10)).apply(new AllWindowFunction<Employee2, List<Employee2>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Employee2> values, Collector<List<Employee2>> out) throws Exception {
                ArrayList<Employee2> model = Lists.newArrayList(values);
                if (model.size() > 0) {
                    System.out.println("10 s内收集到 employee 的数据条数是：" + model.size());
                    out.collect(model);
                }
            }
        })
//                .print();
                .addSink(new SinkToMySQLEmployee2());

        env.execute("flink kafka to Mysql");
    }

    public static void sts(StreamExecutionEnvironment env, Properties props, Model model) throws Exception {
        env.addSource(new FlinkKafkaConsumer011<String>(
            "demo",   //这个 kafka topic 需和生产消息的 topic 一致
            new SimpleStringSchema(),
            props)).setParallelism(1)
            .map(new MapFunction<String, Model>() {
                @Override
                public Model map(String string) throws Exception {
                    Gson gson = new Gson();
                    return gson.fromJson(string, Model.class);
                }
            }).timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Model, List<Model>, TimeWindow>() {
                @Override
                public void apply(TimeWindow window, Iterable<Model> values, Collector<List<Model>> out) throws Exception {
                    ArrayList<Model> model = Lists.newArrayList(values);
                    if (model.size() > 0) {
                        System.out.println("1 分钟内收集到 employee 的数据条数是：" + model.size());
                        out.collect(model);
                    }
                }
            }).addSink(new SinkToMySQLModel());
    }

    public static void sts3(StreamExecutionEnvironment env, Properties props, Employee2 model) throws Exception {
        env.addSource(new FlinkKafkaConsumer011<String>(
                "demo",   //这个 kafka topic 需和生产消息的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(new MapFunction<String, Employee2>() {
                    @Override
                    public Employee2 map(String string) throws Exception {
                        Gson gson = new Gson();
                        return gson.fromJson(string, Employee2.class);
                    }
                }).timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Employee2, List<Employee2>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Employee2> values, Collector<List<Employee2>> out) throws Exception {
                ArrayList<Employee2> model = Lists.newArrayList(values);
                if (model.size() > 0) {
                    System.out.println("1 分钟内收集到 employee 的数据条数是：" + model.size());
                    out.collect(model);
                }
            }
        }).addSink(new SinkToMySQLEmployee2());
    }

    public static void sts2(StreamExecutionEnvironment env, Properties props) throws Exception {
        env.addSource(new FlinkKafkaConsumer011<String>(
                "demo",   //这个 kafka topic 需和生产消息的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String string) throws Exception {
//                        Gson gson = new Gson();
//                        return gson.fromJson(string, String.class);
                        return string;
                    }
                })
                .timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<String, List<String>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<String> values, Collector<List<String>> out) throws Exception {
                ArrayList<String> strings = Lists.newArrayList(values);
                if (strings.size() > 0) {
                    System.out.println("1 分钟内收集到 employee 的数据条数是：" + strings.size());
                    out.collect(strings);
                }
            }
        }).addSink(new SinkToMySQLString());
    }

}
