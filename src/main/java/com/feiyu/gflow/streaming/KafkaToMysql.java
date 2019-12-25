package com.feiyu.gflow.streaming;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

/**
 * 数据落地
 * 将Kafka源表写入MySQL目标表中
 *
 * <p>输入数据源为Kafka里的业务流表
 *
 * <p>用法: <code>LogParser --input &lt;broker/topic&gt; --output &lt;broker&gt;</code><br>
 *
 * <p>示例:
 * <ul>
 * <li>
 * <li>
 * <li>
 * </ul>
 */
public class KafkaToMysql {

    public static void main(String[] args) throws Exception {

        // 检查输入参数
        ParameterTool pt = ParameterTool.fromArgs(args);

        // 检查点配置
        String checkpointDirectory = pt.get("checkpoint.directory", "file:///tmp/cp/");
        long checkpointSecondInterval = pt.getLong("checkpoint.second.interval", 10);

        // 所属应用
        int parserApp = pt.getInt("parser.app", 20027);

        // 输出表配置
        String toMySQLHost = pt.get("to.mysql.host", "10.250.0.38");
        int toMySQLPort = pt.getInt("to.mysql.port", 3306);
        String toMySQLDB = pt.get("to.mysql.db", "xy_data_20027");
        String toMySQLUser = pt.get("to.mysql.user", "public");
        String toMySQLPassword = pt.get("to.mysql.password", "public");
        int toMySQLSecondInterval = pt.getInt("to.mysql.second.interval", 3000);

        // 输入事件流配置
        String fromKafkaBootstrapServers = pt.get("from.kafka.bootstrap.servers", "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092");
        String fromKafkaGroupID = pt.get("from.kafka.group.id", KafkaToMysql.class.getName() + toMySQLDB);

        // 输出链接串
        String toMySQLUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true", toMySQLHost, toMySQLPort, toMySQLDB);

        // 初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据流表
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 使参数在可视化界面有效
        env.getConfig().setGlobalJobParameters(pt);

        // 设置重启策略，5次重试（每次尝试间隔50秒）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,50000));

        // 获取检查点配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 每隔10秒启动一个检查点（检查点周期）
        checkpointConfig.setCheckpointInterval(checkpointSecondInterval * 1000);

        // 设置检查点模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 确保检查点之间有1秒的时间间隔（检查点最小间隔）
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);

        // 检查点必须在10秒之内完成或者被丢弃（检查点超时时间）
        checkpointConfig.setCheckpointTimeout(10000);

        // 同一时间只允许进行一次检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 表示一旦任务被取消后会保留检查点数据，以便根据实际需要恢复到指定的检查点
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置状态后端（将检查点持久化），默认保存在内存中
        env.setStateBackend((StateBackend) new FsStateBackend(checkpointDirectory, true));

        // 设置kafka消费参数
        Properties props = new Properties();

        // 自定义配置信息
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, fromKafkaBootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, fromKafkaGroupID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 定义事件日志流
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<>("d_device", new SimpleStringSchema(), props)).name("kafka source device").setParallelism(1);

        String queryString = "INSERT INTO `xy_data_20027`.`d_device`(`os`, `v_day`, `v_hour`, `v_time`, `did`, `screen`, `osv`, `hd`, `gv`, `idfa`, `imei`, `mac`, `channel_id`, `ip`, `sid`, `isbreak`, `ispirated`, `adid`, `wid`, `country_id`, `province_id`, `city_id`, `ext`, `original_channel_id`) VALUES (2, 20190930, 2019093009, 1569807580, " +
                "'b4c0ec2e920badadebb14f1c1fa0ba6b', " +
                "'1080*1920', '7.0', 'Xiaomi-Redmi Note 4X', '1.7', '', '99001064345182', '04:b1:67:9c:66:54', '270048', '14.106.189.210', 1, 0, 0, 0, 0, 168, 27, 383, '', '270048');";
        inputStream.addSink(new KafkaToMysql.MysqlSinkForFreeQuery(toMySQLHost, toMySQLPort, toMySQLDB, toMySQLUser, toMySQLPassword, toMySQLSecondInterval, queryString)).setParallelism(1);

        // 执行任务
        env.execute(KafkaToMysql.class.getName());
    }

    // 通过mysql查询语句从数据看中定时获取配置数据产生一批数据源
    public static final class MysqlSinkForFreeQuery extends RichSinkFunction<String> implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final Logger log = LoggerFactory.getLogger(KafkaToMysqlOld.MySQLConfigSource.class);

        private String host;
        private Integer port;
        private String db;
        private String user;
        private String password;
        private Integer secondInterval;
        private String queryString;

        private volatile boolean isRunning = true;

        private Connection connection;

        public MysqlSinkForFreeQuery(String host, Integer port, String db, String user, String password, Integer secondInterval, String queryString) {
            this.host = host;
            this.port = port;
            this.db = db;
            this.user = user;
            this.password = password;
            this.secondInterval = secondInterval;
            this.queryString = queryString;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            log.info("open custom source from mysql.");
            super.open(parameters);

            connection = null;

            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                log.error("com.mysql.jdbc.Driver not found.");
                e.printStackTrace();
            }

            try {
                connection = DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/"+db+"?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true", user, password);
                log.info("connection to mysql is success, id is {}.", connection);
            } catch (Exception e){
                log.error("connection to mysql is error, config is {host: " + host + ", port: " + port + ", db: " + db + ", user: " + user);
            }
        }

        @Override
        public void close() throws Exception {
            log.info("close custom source from mysql.");
            super.close();

            if (connection != null) {
                connection.close();
            }
        }

        public String invoke() {
            log.info("run custom source from mysql.");
            try {
                // 查询应用配置
                PreparedStatement preparedStatement = connection.prepareStatement(queryString);
                int resultNum = preparedStatement.executeUpdate();


                // 每隔多少秒执行一次查询
//                Thread.sleep(1000 * secondInterval);

            } catch (Exception ex){
                log.error("query config from mysql is failed, error: ", ex);
            }
            return "";
        }

    }

}
