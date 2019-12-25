package com.feiyu.gflow.streaming;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

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
public class DataLoading {

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
        String fromKafkaGroupID = pt.get("from.kafka.group.id", DataLoading.class.getName() + toMySQLDB);

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

        // 定义Kafka源表
        tEnv.connect(new Kafka().version("0.11").topic("d_device").startFromEarliest().property("zookeeper.connect", "10.250.0.101:2181,10.250.0.102:2181,10.250.0.103:2181").property("bootstrap.servers", fromKafkaBootstrapServers))
                // declare a format for this system
                .withFormat(new Json().schema(org.apache.flink.table.api.Types.ROW(new String[]{"os", "v_day", "v_hour", "v_time", "did", "screen", "osv", "hd", "gv", "idfa", "imei", "mac", "channel_id", "ip", "sid", "isbreak", "ispirated", "adid", "wid", "country_id", "province_id", "city_id", "ext", "original_channel_id", "ng_time", "w_time", "country_iso_code", "subdivision_iso_code", "city_name", "app_id"},
                        new TypeInformation[]{
                                org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(),
                                org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(),
                                org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(),
                                org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(),
                                org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(),
                                org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(),
                                org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(),
                                org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.INT()
                })).failOnMissingField(true))
                // declare the schema of the table
                .withSchema(new Schema()
                        .field("os", Types.INT)
                        .field("v_day", Types.INT)
                        .field("v_hour", Types.INT)
                        .field("v_time", Types.INT)
                        .field("did", Types.STRING)
                        .field("screen", Types.STRING)
                        .field("osv", Types.STRING)
                        .field("hd", Types.STRING)
                        .field("gv", Types.STRING)
                        .field("idfa", Types.STRING)
                        .field("imei", Types.STRING)
                        .field("mac", Types.STRING)
                        .field("channel_id", Types.STRING)
                        .field("ip", Types.STRING)
                        .field("sid", Types.INT)
                        .field("isbreak", Types.INT)
                        .field("ispirated", Types.INT)
                        .field("adid", Types.INT)
                        .field("wid", Types.INT)
                        .field("country_id", Types.INT)
                        .field("province_id", Types.INT)
                        .field("city_id", Types.INT)
                        .field("ext", Types.STRING)
                        .field("original_channel_id", Types.STRING)
                        .field("ng_time", Types.INT)
                        .field("w_time", Types.INT)
                        .field("country_iso_code", Types.STRING)
                        .field("subdivision_iso_code", Types.STRING)
                        .field("city_name", Types.STRING)
                        .field("app_id", Types.INT)
                )
                // specify the update-mode for streaming tables
                .inAppendMode()
                // register as source, sink, or both and under a name
                .registerTableSource("d_device");

        // 定义MySQL目标表
        tEnv.registerTableSink(parserApp + "d_device", new JDBCAppendTableSinkBuilder()
                .setDBUrl(toMySQLUrl)
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername(toMySQLUser)
                .setPassword(toMySQLPassword)
                .setBatchSize(toMySQLSecondInterval)
//                .setQuery("INSERT IGNORE INTO d_device (v_time,channel_id,did,app_id) values(?,?,?,?)")
                .setQuery("INSERT IGNORE INTO d_device (`os`, `v_day`, `v_hour`, `v_time`, `did`, `screen`, `osv`, `hd`, `gv`, `idfa`, `imei`, `mac`, `channel_id`, `ip`, `sid`, `isbreak`, `ispirated`, `adid`, `wid`, `country_id`, `province_id`, `city_id`, `ext`, `original_channel_id`, `ng_time`, `w_time`, `country_iso_code`, `subdivision_iso_code`, `city_name`) " +
                        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .setParameterTypes(new TypeInformation[]{org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING(), org.apache.flink.table.api.Types.STRING()})
                .build()
                .configure(new String[]{"os", "v_day", "v_hour", "v_time", "did", "screen", "osv", "hd", "gv", "idfa", "imei", "mac", "channel_id", "ip", "sid", "isbreak", "ispirated", "adid", "wid", "country_id", "province_id", "city_id", "ext", "original_channel_id", "ng_time", "w_time", "country_iso_code", "subdivision_iso_code", "city_name"},
                        new TypeInformation[]{Types.INT, Types.INT, Types.INT, Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.INT, Types.STRING, Types.STRING, Types.INT, Types.INT, Types.STRING, Types.STRING, Types.STRING})
        );

        // 将源表（Kafka）读取插入到目标表（MySQL）
        tEnv.scan("d_device").where("app_id="+parserApp)
                .select("os, v_day, v_hour, v_time, did, screen, osv, hd, gv, idfa, imei, mac, channel_id, ip, sid, isbreak, ispirated, adid, wid, country_id, province_id, city_id, ext, original_channel_id, ng_time, w_time, country_iso_code, subdivision_iso_code, city_name")
                .insertInto(parserApp + "d_device");

        // 执行任务
        env.execute(DataLoading.class.getName());
    }
}
