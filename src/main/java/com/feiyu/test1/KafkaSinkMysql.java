package com.feiyu.test1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.sql.Types;
import java.util.Properties;

public class KafkaSinkMysql {

    public static void main(String[] args) throws Exception {

//        // 检查输入参数
//        ParameterTool pt = ParameterTool.fromArgs(args);
//        // 无效事件日志输出目录（签名拒绝、参数不全、未知接口等）
//        String toBasePath = pt.get("to.base.path", "file:///tmp/log/");
//        // 检查点配置
//        String checkpointDirectory = pt.get("checkpoint.directory", "file:///tmp/cp/");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

//        // 检查输入参数
//        ParameterTool pt = ParameterTool.fromArgs(args);
//        // 检查点配置
//        String checkpointDirectory = pt.get("checkpoint.directory", "file:///tmp/cp/");
//        long checkpointSecondInterval = pt.getLong("checkpoint.second.interval", 10);
//
//        // 获取检查点配置
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        // 每隔10秒启动一个检查点（检查点周期）
//        checkpointConfig.setCheckpointInterval(checkpointSecondInterval * 1000);
//        // 设置检查点模式
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 确保检查点之间有1秒的时间间隔（检查点最小间隔）
//        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
//        // 检查点必须在10秒之内完成或者被丢弃（检查点超时时间）
//        checkpointConfig.setCheckpointTimeout(10000);
//        // 同一时间只允许进行一次检查点
//        checkpointConfig.setMaxConcurrentCheckpoints(1);
//        // 表示一旦任务被取消后会保留检查点数据，以便根据实际需要恢复到指定的检查点
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 设置状态后端（将检查点持久化），默认保存在内存中
//        env.setStateBackend((StateBackend) new FsStateBackend(checkpointDirectory, true));


//        env.getConfig().enableForceKryo();
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092");
        props.put("group.id", "test");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "latest");//earliest

        FlinkKafkaConsumer011 kafkaSource = new FlinkKafkaConsumer011<String>(
                "zzz",   //这个 kafka topic 需和生产消息的 topic 一致
                new SimpleStringSchema(),
                props);

        MySQLTwoPhaseCommitSink mysqlSink = new MySQLTwoPhaseCommitSink(
                "jdbc:mysql://10.250.0.38:3306/xy_data_20027?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true" ,
                "com.mysql.jdbc.Driver",
                "public",
                "public",
                "insert into employee_lwn (id, name, password, age, salary, department) values (?, ?, ?, ?, ?, ?)",
                (new int[]{Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.VARCHAR}));
//                (new int[]{Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR,Types.VARCHAR}));


//        MapFunction mf = new MapFunction<String, Row>() {
//            @Override
//            public Row map(String string) throws Exception {
//                Gson gson = new Gson();
//                Employee2 employee = gson.fromJson(string, Employee2.class);
//                Row rowAddr = new Row(6);
//                rowAddr.setField(0, employee.id());
//                rowAddr.setField(1, employee.name());
//                rowAddr.setField(2, employee.password());
//                rowAddr.setField(3, employee.age());
//                rowAddr.setField(4, employee.salary());
//                rowAddr.setField(5, employee.department());
//                return rowAddr;
//            }
//        };
//
//        AllWindowFunction awf = new AllWindowFunction<Row, List<Row>, TimeWindow>() {
//            @Override
//            public void apply(TimeWindow window, Iterable<Row> values, Collector<List<Row>> out) throws Exception {
//                ArrayList<Row> model = Lists.newArrayList(values);
//                if (model.size() > 0) {
//                    System.out.println("10 秒内收集到 employee 的数据条数是：" + model.size());
//                    out.collect(model);
//                }
//            }
//        };

        env.addSource(kafkaSource).setParallelism(1)
        .map(new MF()).timeWindowAll(Time.seconds(10)).apply(new AWF())
//        .print()
        .addSink(mysqlSink)
        .setParallelism(1);

        env.execute("flink kafka to Mysql");
    }


}
