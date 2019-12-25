package com.feiyu.test2;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.sql.Types;
import java.util.Properties;

public class KafkaSinkMysql {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //检查点配置
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //kafka配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092");
        props.put("group.id", "test");
        props.put("auto.offset.reset", "latest");//earliest

        env.addSource(new FlinkKafkaConsumer011<>(
                "zzz",   //这个 kafka topic 需和生产消息的 topic 一致
                new JSONKeyValueDeserializationSchema(true),
                props)).setParallelism(1)
                .timeWindowAll(Time.seconds(5))
                .apply(new AWF()).setParallelism(1)
                .addSink(new MySQLTwoPhaseCommitSink(
                        "jdbc:mysql://10.250.0.38:3306/xy_data_20027?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true" ,//failOverReadOnly=false
                        "com.mysql.jdbc.Driver",
                        "public",
                        "public",
                        "insert into employee_lwn (id, name, password, age, salary, department) values (?, ?, ?, ?, ?, ?)",
                        (new int[]{Types.INTEGER,Types.VARCHAR,Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.VARCHAR})))
                .setParallelism(1);

        env.execute("flink kafka to Mysql");
    }


}
