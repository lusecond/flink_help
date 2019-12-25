package com.feiyu.gflow.streaming;

import com.feiyu.gflow.util.StringUtil;
import com.google.gson.Gson;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.join;

/**
 * 实现将指定Kafka收集的Nginx日志落地到Kafka表
 *
 * <p>输入数据源为Kafka里的nginx日志
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
public class KafkaToMysqlOld {

    // 定义配置描述
    private static final MapStateDescriptor<String, Map<String, String>> configDescriptor = new MapStateDescriptor<>("config", Types.STRING, Types.MAP(Types.STRING, Types.STRING));

    // 签名拒绝的日志
    private static final OutputTag<DateElement> rejectedLogTag = new OutputTag<DateElement>("rejected") {};

    // 参数不全的日志
    private static final OutputTag<DateElement> incompleteLogTag = new OutputTag<DateElement>("incomplete") {};

    // 开放事件日志
    private static final OutputTag<DateElement> openLogTag = new OutputTag<DateElement>("open") {};

    // 激活事件日志
    private static final OutputTag<String> activityLogTag = new OutputTag<String>("activity") {};

    // 用户事件日志
    private static final OutputTag<String> userLogTag = new OutputTag<String>("user") {};

    // 账号登录事件日志
    private static final OutputTag<String> accountLoginLogTag = new OutputTag<String>("account-login") {};

    // 进入游戏事件日志
    private static final OutputTag<String> enterGameLogTag = new OutputTag<String>("enter-game") {};

    // 登录事件日志
    private static final OutputTag<String> loginLogTag = new OutputTag<String>("login") {};

    // 角色事件日志
    private static final OutputTag<String> playerLogTag = new OutputTag<String>("player") {};

    // 角色升级日志
    private static final OutputTag<String> playerUpdateLogTag = new OutputTag<String>("player-update") {};

    // 支付事件日志
    private static final OutputTag<String> payLogTag = new OutputTag<String>("pay") {};

    // 关卡事件日志
    private static final OutputTag<String> missionLogTag = new OutputTag<String>("mission") {};

    // 通用事件日志
    private static final OutputTag<String> eventLogTag = new OutputTag<String>("event") {};

    // 记录事件日志
    private static final OutputTag<String> recordLogTag = new OutputTag<String>("record") {};

    // 异常事件日志
    private static final OutputTag<String> exceptLogTag = new OutputTag<String>("except") {};

    // 内购事件日志
    private static final OutputTag<String> iapLogTag = new OutputTag<String>("iap") {};

    // 客户端Debug事件日志
    private static final OutputTag<String> clientDebugLogTag = new OutputTag<String>("client-debug") {};

    // 其他事件日志
    private static final OutputTag<DateElement> otherLogTag = new OutputTag<DateElement>("other") {};

    public static void main(String[] args) throws Exception {

        // 检查输入参数
        ParameterTool pt = ParameterTool.fromArgs(args);

        // 无效事件日志输出目录（签名拒绝、参数不全、未知接口等）
        String toBasePath = pt.get("to.base.path", "file:///tmp/log/");

        // 检查点配置
        String checkpointDirectory = pt.get("checkpoint.directory", "file:///tmp/cp/");
        long checkpointSecondInterval = pt.getLong("checkpoint.second.interval", 10);

        // GeoIP 地址库地址
        String geoIpDatabase = pt.get("geo.ip.database.path", "/Users/lusecond/Library/Containers/com.tencent.WeWorkMac/Data/Library/Application Support/WXWork/Data/1688854096298490/Cache/File/2019-12/gflow/src/main/resources/GeoLite2-City.mmdb");

        // 输入事件流配置
        String fromKafkaBootstrapServers = pt.get("from.kafka.bootstrap.servers", "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092");

//        String fromLoginKafkaGroupID = pt.get("from.kafka.group.id.login", "login");
//        String fromPayKafkaGroupID = pt.get("from.kafka.group.id.pay", "pay");
//        String fromPlayerKafkaGroupID = pt.get("from.kafka.group.id.player", "player");
//        String fromUserKafkaGroupID = pt.get("from.kafka.group.id.user", "user");
//        String fromDeviceKafkaGroupID = pt.get("from.kafka.group.id.device", "device");
//        String fromOldDevieKafkaGroupID = pt.get("from.kafka.group.id.olddevice", "olddevice");
//        String fromAccountLoginKafkaGroupID = pt.get("from.kafka.group.id.accountlogin", "accountlogin");
//
//        String fromLoginKafkaTopic = pt.get("from.kafka.topic.login", "d_login");
//        String fromPayKafkaTopic = pt.get("from.kafka.topic.pay", "d_pay");
//        String fromPlayerKafkaTopic = pt.get("from.kafka.topic.player", "d_player");
//        String fromUserKafkaTopic = pt.get("from.kafka.topic.user", "d_user");
//        String fromDeviceKafkaTopic = pt.get("from.kafka.topic.device", "d_device");
//        String fromOldDeviceKafkaTopic = pt.get("from.kafka.topic.olddevice", "d_device");
//        String fromAccountLoginKafkaTopic = pt.get("from.kafka.topic.accountlogin", "d_accountlogin");

        // 输出表配置
//        String toKafkaBootstrapServers = pt.get("to.kafka.bootstrap.servers", "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092");

        // 配置流配置
//        String configMySQLHost = pt.get("config.mysql.host", "gz-cdb-nqq556yz.sql.tencentcdb.com");
//        int configMySQLPort = pt.getInt("config.mysql.port", 60667);
//        String configMySQLDB = pt.get("config.mysql.db", "xy_center");
//        String configMySQLUser = pt.get("config.mysql.user", "root");
//        String configMySQLPassword = pt.get("config.mysql.password", "z7ehXRKysFUDmTEaT75G");
//        int configMySQLSecondInterval = pt.getInt("config.mysql.second.interval", 30);

        //开发环境mysql xy_center
        String configMySQLHost = pt.get("config.mysql.host", "10.250.0.38");
        int configMySQLPort = pt.getInt("config.mysql.port", 3306);
        String configMySQLDB = pt.get("config.mysql.db", "xy_center");
        String configMySQLUser = pt.get("config.mysql.user", "public");
        String configMySQLPassword = pt.get("config.mysql.password", "public");
        int configMySQLSecondInterval = pt.getInt("config.mysql.second.interval", 30);

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
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置状态后端（将检查点持久化），默认保存在内存中
        env.setStateBackend((StateBackend) new FsStateBackend(checkpointDirectory, true));

        // 选择设置事件事件和处理事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 设置kafka消费参数
        Properties props = new Properties();

        // 自定义配置信息
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, fromKafkaBootstrapServers);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, fromLoginKafkaGroupID);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 分区自动发现周期
//        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        // 定义事件日志流
//        DataStream<String> loginInputStream = env.addSource(new FlinkKafkaConsumer011<>(fromLoginKafkaTopic, new SimpleStringSchema(), props)).name("login kafka stream").setParallelism(1);
//        DataStream<String> payInputStream = env.addSource(new FlinkKafkaConsumer011<>(fromPayKafkaTopic, new SimpleStringSchema(), props)).name("pay kafka stream").setParallelism(1);
//        DataStream<String> playerInputStream = env.addSource(new FlinkKafkaConsumer011<>(fromPlayerKafkaTopic, new SimpleStringSchema(), props)).name("player kafka stream").setParallelism(1);
//        DataStream<String> userInputStream = env.addSource(new FlinkKafkaConsumer011<>(fromUserKafkaTopic, new SimpleStringSchema(), props)).name("user kafka stream").setParallelism(1);
//        DataStream<String> deviceInputStream = env.addSource(new FlinkKafkaConsumer011<>(fromDeviceKafkaTopic, new SimpleStringSchema(), props)).name("device kafka stream").setParallelism(1);
//        DataStream<String> oldDeviceInputStream = env.addSource(new FlinkKafkaConsumer011<>(fromOldDeviceKafkaTopic, new SimpleStringSchema(), props)).name("olddevice kafka stream").setParallelism(1);
//        DataStream<String> accountLoginInputStream = env.addSource(new FlinkKafkaConsumer011<>(fromAccountLoginKafkaTopic, new SimpleStringSchema(), props)).name("acconutlogin kafka stream").setParallelism(1);

        // 定义配置流（周期性地从数据看中获取配置并广播出去）
//        DataStreamSource<Tuple2<String, Map<String, String>>> configStream = env.addSource(new MySQLConfigSource(configMySQLHost, configMySQLPort, configMySQLDB, configMySQLUser, configMySQLPassword, configMySQLSecondInterval)).setParallelism(1);
//        configStream.print();

        initMysqlTable(env, pt);


//        for k, v in configStream:

//        configStream.map(new MapFunction<Tuple2<String, Map<String, String>>, Object>() {
//            @Override
//            public Object map(Tuple2<String, Map<String, String>> value) throws Exception {
////                // 定义MySQL目标表
////                tEnv.registerTableSink(value.f0 + "_" + "d_device", new JDBCAppendTableSinkBuilder()
////                    .setDBUrl(toMySQLUrl)
////                    .setDrivername("com.mysql.jdbc.Driver")
////                    .setUsername(toMySQLUser)
////                    .setPassword(toMySQLPassword)
////                    .setBatchSize(toMySQLSecondInterval)
////                    .setQuery("INSERT IGNORE INTO d_device (`os`, `v_day`, `v_hour`, `v_time`, `did`, `screen`, `osv`, `hd`, `gv`, `idfa`, `imei`, `mac`, `channel_id`, `ip`, `sid`, `isbreak`, `ispirated`, `adid`, `wid`, `country_id`, `province_id`, `city_id`, `ext`, `original_channel_id`) " +
////                            "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
////                    .setParameterTypes(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING})
////                    .build()
////                    .configure(new String[]{"v_time", "channel_id", "did", "app_id"},
////                            new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING})
////                );
//                return value.f0;
//            }
//        });


        // 将配置流广播
//        BroadcastStream<Tuple2<String, Map<String, String>>> broadcastConfigStream = configStream.name("mysql config stream").broadcast(configDescriptor);


        // 将事件流和广播的配置流连接
//        BroadcastConnectedStream<String, Tuple2<String, Map<String, String>>> connectedStream = inputStream.connect(broadcastConfigStream);

//        // 内购检测的请求
//        outputStream.getSideOutput(iapLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_iap", new SimpleStringSchema())).name("iap log sink").setParallelism(1);
//
//        // 开放接口的请求
//        outputStream.getSideOutput(openLogTag).addSink(StreamingFileSink
//                .forRowFormat(new Path(toBasePath + File.separator + "other"), new SimpleStringEncoder<DateElement>("UTF-8"))
//                .withBucketAssigner(new DateBucketAssigner())
//                .withBucketCheckInterval(1000L)
//                .build()).name("open log sink").setParallelism(1);
//
//        // 创角数据
//        outputStream.getSideOutput(playerLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_player", new SimpleStringSchema())).name("player log sink").setParallelism(1);
//
//        // 升级数据
//        outputStream.getSideOutput(playerUpdateLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_player_level", new SimpleStringSchema())).name("player level log sink").setParallelism(1);
//
//        // 事件数据
//        outputStream.getSideOutput(eventLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_event", new SimpleStringSchema())).name("event log sink").setParallelism(1);
//
//        // 支付数据
//        outputStream.getSideOutput(payLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_pay", new SimpleStringSchema())).name("pay log sink").setParallelism(1);
//
//        // 用户数据
//        outputStream.getSideOutput(userLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_user", new SimpleStringSchema())).name("user log sink").setParallelism(1);
//
//        // 客户端调试记录
//        outputStream.getSideOutput(clientDebugLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_sdk_debug", new SimpleStringSchema())).name("sdk log sink").setParallelism(1);
//
//        // 进入游戏数据
//        outputStream.getSideOutput(enterGameLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_enter_game", new SimpleStringSchema())).name("enter game log sink").setParallelism(1);
//
//        // 关卡数据
//        outputStream.getSideOutput(missionLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_mission", new SimpleStringSchema())).name("mission log sink").setParallelism(1);
//
//        // 账号登录
//        outputStream.getSideOutput(accountLoginLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_account_login", new SimpleStringSchema())).name("account login log sink").setParallelism(1);
//
//        // 将设备激活事件记录Sink到Kafka
//        outputStream.getSideOutput(activityLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_device", new SimpleStringSchema())).name("device log sink").setParallelism(1);
//
//        // 将登录游戏事件记录Sink到Kafka
//        outputStream.getSideOutput(loginLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_login", new SimpleStringSchema())).name("login log sink").setParallelism(1);
//
//        // 游戏记录数据
//        outputStream.getSideOutput(recordLogTag).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "d_record", new SimpleStringSchema())).name("record log sink").setParallelism(1);
//
//        // 非法的请求
//        outputStream.addSink(StreamingFileSink
//                .forRowFormat(new Path(toBasePath + File.separator + "illegal"), new SimpleStringEncoder<String>("UTF-8"))
//                .withBucketCheckInterval(1000L)
//                .build()).name("illegal log sink").setParallelism(1);

        // 执行任务
        env.execute(KafkaToMysqlOld.class.getName());
    }

    public static void initMysqlTable(StreamExecutionEnvironment env, ParameterTool pt) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String configMySQLHost = pt.get("config.mysql.host", "10.250.0.38");
        int configMySQLPort = pt.getInt("config.mysql.port", 3306);
        String configMySQLDB = pt.get("config.mysql.db", "xy_center");
        String configMySQLUser = pt.get("config.mysql.user", "public");
        String configMySQLPassword = pt.get("config.mysql.password", "public");
        int configMySQLSecondInterval = pt.getInt("config.mysql.second.interval", 30);

//        String queryString = "select app_id from xy_center.xy_game where etl_version = 2";
//        DataStreamSource<List<Map<String, String>>> appIds = env.addSource(new MySQLConfigSourceFromFreeQuery(configMySQLHost, configMySQLPort, configMySQLDB, configMySQLUser, configMySQLPassword, configMySQLSecondInterval, queryString)).setParallelism(1);
////        appIds.print();
//        DataStream<List<String>> bcd = appIds.map(
//            new MapFunction<List<Map<String, String>>, List<String>>() {
//                @Override
//                public List<String> map(List<Map<String, String>> value) throws Exception {
//                    List<String> abc = new ArrayList<>();
//                    for (int i = 0; i < value.size(); i++) {
//                        abc.add(value.get(i).get("app_id"));
//                    }
//                    return abc;
//                }
//            }
//        );
//        bcd.print();
//        System.out.println(join(bcd, ","));

//        String queryString2 = "select * from xy_center.xy_db where db = 'xy_data' and app_id in ("+ join(abc, ",") +")";
        String queryString2 = "select * from xy_center.xy_db where db = 'xy_data' and app_id in ("+ "20001,20027,20035" +")";
        DataStreamSource<List<Map<String, String>>> test = env.addSource(new MySQLConfigSourceFromFreeQuery(configMySQLHost, configMySQLPort, configMySQLDB, configMySQLUser, configMySQLPassword, configMySQLSecondInterval, queryString2)).setParallelism(1);
//        test.print();
        DataStream<List<String>> bcd = test.map(
            new MapFunction<List<Map<String, String>>, List<String>>() {
                @Override
                public List<String> map(List<Map<String, String>> value) throws Exception {
                    List<String> abc = new ArrayList<>();
                    for (int i = 0; i < value.size(); i++) {
                        abc.add(value.get(i).get("app_id"));

                        String password = value.get(i).get("password");
                        String port = value.get(i).get("port");
                        String host = value.get(i).get("host");
                        String app_id = value.get(i).get("app_id");
                        String user = value.get(i).get("user");
                        String db = value.get(i).get("db");

                        int toMySQLSecondInterval = pt.getInt("to.mysql.second.interval", 3000);
                        String toMySQLUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true", host, port, db);

//                        // 定义MySQL目标表
//                        tEnv.registerTableSink(app_id + "d_device", new JDBCAppendTableSinkBuilder()
//                                        .setDBUrl(toMySQLUrl)
//                                        .setDrivername("com.mysql.jdbc.Driver")
//                                        .setUsername(user)
//                                        .setPassword(password)
//                                        .setBatchSize(toMySQLSecondInterval)
////                .setQuery("INSERT IGNORE INTO d_device (v_time,channel_id,did,app_id) values(?,?,?,?)")
//                                        .setQuery("INSERT IGNORE INTO d_device (`os`, `v_day`, `v_hour`, `v_time`, `did`, `screen`, `osv`, `hd`, `gv`, `idfa`, `imei`, `mac`, `channel_id`, `ip`, `sid`, `isbreak`, `ispirated`, `adid`, `wid`, `country_id`, `province_id`, `city_id`, `ext`, `original_channel_id`) " +
//                                                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
//                                        .setParameterTypes(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING})
//                                        .build()
//                                        .configure(new String[]{"v_time", "channel_id", "did", "app_id"},
//                                                new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING})
//                        );

                    }
                    return abc;
                }
            }
        );

    }

    // 日志清洗并分流
    public static final class LogCleaning extends BroadcastProcessFunction<String, Tuple2<String, Map<String, String>>, String> implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final Logger log = LoggerFactory.getLogger(LogCleaning.class);

        // 地址库
        private DatabaseReader reader;
        private String geoIpDatabase;

        // 日志正则
        private String regex = "^(.*?) \\[(.*? \\+0800)] (\\d+-report-nebula\\.737\\.com) \"(POST|GET) (/v\\d+/.*?) HTTP/1\\.1\" \\d+ \\d+ \"[^\"]*\" \"[^\"]*\" \"([^\"]*)\" \\d+\\.\\d+ ([a-f0-9]+)$";
        private Pattern pattern;

        // 清除参数列表
        private List<String> cleaningItemList = Arrays.asList("_u", "post", "sign", "signtest");

        // 转义数据处理器
        private Gson gson;

        private LogCleaning(String geoIpDatabase) {
            this.geoIpDatabase = geoIpDatabase;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.reader = new DatabaseReader.Builder(new File(this.geoIpDatabase)).build();
            this.pattern = Pattern.compile(this.regex);
            this.gson = new Gson();
        }

        /**
         * 处理广播流中的每一条数据并更新配置状态
         *
         * @param element       广播流中的数据
         * @param context       上下文
         * @param collector     输出零条或多条数据
         * @throws Exception    Super
         */
        @Override
        public void processBroadcastElement(Tuple2<String, Map<String, String>> element, Context context, Collector<String> collector) throws Exception {
            BroadcastState<String, Map<String, String>> broadcastState = context.getBroadcastState(configDescriptor); // 获取状态
            broadcastState.put(element.f0, element.f1); // 更新状态
        }

        /**
         * 从广播状态中读取配置并基于配置处理事件流中的数据
         *
         * @param element       事件流中的数据
         * @param context       上下文
         * @param collector     输出零条或多条数据
         * @throws Exception    ...
         */
        @Override
        public void processElement(String element, ReadOnlyContext context, Collector<String> collector) throws Exception {

            // 从广播流中获取配置
            ReadOnlyBroadcastState<String, Map<String, String>> broadcastState = context.getBroadcastState(configDescriptor);

            try {

                // 正则匹配日志
                Matcher m = this.pattern.matcher(element);
                if (m.find()) {

                    // 定义数据字典
                    Map<String, String> r = new HashMap<>();

                    // 设置时区
                    TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");
                    Calendar calendar = Calendar.getInstance(timeZone);

                    // 定义一个字符串数组
                    String[] ts;

                    // 获取客户端地址
                    ts = m.group(1).split(" ");
                    String ipAddress = ts[1].equals("-") ? ts[0] : ts[1].split(",")[0];

                    // 数据记录客户端地址
                    r.put("ip", ipAddress);

                    // 获取请求时间
                    SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH);
                    Date requestDate = dateFormat.parse(m.group(2));

                    // 获取日期
                    String requestDateString = new SimpleDateFormat ("yyyy-MM-dd").format(requestDate);

                    // 获取请求地址
                    String requestUrl = m.group(5);

                    // 获取请求内容
                    String requestBody = m.group(6);

                    // 如果请求为GET方式，解析出请求地址及请求内容
                    if (m.group(4).equals("GET")) {
                        ts = requestUrl.split("\\?");
                        if (ts.length == 2) {
                            requestUrl = ts[0];
                            requestBody = ts[1];
                        }
                    }

                    // 获取请求业务
                    ts = requestUrl.split("/");
                    String requestBusiness = ts.length == 3 ? ts[2] : "";

                    // 获取所属应用
                    ts = m.group(3).split("-");
                    String app = ts.length == 3 ? ts[0] : "";

                    // 应用不存在
                    if (!broadcastState.contains(app)) {
                        collector.collect(element);
                        return;
                    }

                    // 获取应用配置
                    Map<String, String> config = broadcastState.get(app);

                    // 解析请求内容
                    String[] pairs = requestBody.split("&");

                    // 获取签名密钥
                    String appKey = config.get("appKey");

                    // 将数据进行排序
                    Arrays.sort(pairs);

                    // 签名串
                    String sign = "";

                    // 参数键值
                    String pairKey;
                    String pairValue;

                    // 定义待签名串
                    List<String> sources = new ArrayList<>();

                    // 处理请求数据
                    for (String pair : pairs) {
                        ts = pair.split("=");

                        pairKey = ts[0];
                        pairValue = ts.length == 2 ? URLDecoder.decode(ts[1], "UTF-8") : "";

                        // 上报签名
                        if (pairKey.equals("sign")) {
                            sign = pairValue;
                            continue;
                        }

                        // 过滤参数
                        if (cleaningItemList.contains(ts[0])) {
                            continue;
                        }

                        // 记录数据
                        r.put(pairKey, pairValue);

                        // 添加到待签名串
                        sources.add(URLDecoder.decode(pair, "UTF-8"));
                    }

                    // 签名无效
                    if (!StringUtil.md5(StringUtil.rawURLEncode(String.join("&", sources)) + "&" + appKey).equals(sign)) {
                        context.output(rejectedLogTag, new DateElement(requestDateString, element));
                        return;
                    }

                    // 获取客户端地址所属地区信息
                    InetAddress inetAddress = InetAddress.getByName(ipAddress);
                    CityResponse response = reader.city(inetAddress);

                    r.put("country_iso_code", response.getCountry().getIsoCode());
                    r.put("subdivision_iso_code", response.getMostSpecificSubdivision().getIsoCode());
                    r.put("city_name", response.getCity().getNames().get("zh-CN") != null ? response.getCity().getNames().get("zh-CN") : response.getCity().getName());

                    // 上报时间
                    r.put("r_time", String.format("%d", requestDate.getTime() / 1000));

                    // 批量上报不做时间预处理
                    if (!requestBusiness.equals("mission") && !requestBusiness.equals("record"))
                    {
                        // 事件上报时间
                        if (r.containsKey("time") && !r.get("time").equals("")) {
                            requestDate = new Date(Long.parseLong(r.get("time")) * 1000);
                        }

                        // 支付上报切换
                        if (r.containsKey("pay_time") && !r.get("pay_time").equals("")) {
                            requestDate = new Date(Long.parseLong(r.get("pay_time")) * 1000);
                        }

                        // 事件上报事件
                        r.put("v_time", String.format("%d", requestDate.getTime() / 1000));
                        r.remove("time");

                        // 事件上报月份
                        String month = new SimpleDateFormat("yyyyMM").format(requestDate);
                        r.put("v_month", month);

                        // 事件上报年周
                        calendar.setTime(requestDate);
                        int week = calendar.get(Calendar.WEEK_OF_YEAR);
                        if (week < 10) {
                            r.put("v_week", String.format("%s0%d", month.substring(0, 4), week));
                        } else {
                            r.put("v_week", String.format("%s%d", month.substring(0, 4), week));
                        }

                        // 事件上报日期
                        String day = new SimpleDateFormat("yyyyMMdd").format(requestDate);
                        r.put("v_day", day);

                        // 事件上报小时
                        String hour = new SimpleDateFormat("yyyyMMddHH").format(requestDate);
                        r.put("v_hour", hour);
                    }

                    switch (requestBusiness) {

                        // 激活事件逻辑处理（PHP逻辑确认完成）
                        // TODO 广告逻辑屏蔽
                        // {"app_memory":"0","ip":"115.183.92.86","idfa":"","screen":"810*1440","v_hour":"2019112310","avali_memory":"0","mac":"","gv":"1.0","osv":"6.0.1","country_iso_code":"CN","v_week":"201947","tatol_memory":"0","v_month":"201911","v_day":"20191123","imei":"980000000098602","v_time":1574477409,"hd":"Netease-MuMu","subdivision_iso_code":"BJ","app_id":"20027","channel_id":"270082","did":"f037e23940087a585fbc390685d89f24"}
                        case "activity":

                            if (r.get("did").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            String idfa = r.getOrDefault("idfa", "");
                            if (idfa.length() < 32 || !StringUtil.pregMatch("([a-zA-Z0-9\\-]+)", idfa)) {
                                idfa = "";
                            }

                            if (!idfa.equals("") && !idfa.contains("-")) {
                                idfa = String.join("-", new String[] {idfa.substring(0, 8), idfa.substring(8, 12), idfa.substring(12, 16), idfa.substring(16, 20), idfa.substring(20)});
                            }

                            r.put("idfa", idfa.toUpperCase());


                            String mac = r.getOrDefault("mac", "");
                            if (mac.length() < 12 || !StringUtil.pregMatch("([0-9a-zA-Z:]+)", idfa)) {
                                mac = "";
                            }

                            if (!mac.equals("") && !mac.contains(":")) {
                                mac = String.join("-", new String[] {mac.substring(0, 2), mac.substring(2, 4), mac.substring(4, 6), mac.substring(6, 8), mac.substring(8, 10), mac.substring(10)});
                            }

                            r.put("mac", mac.toLowerCase());


                            String imei = r.getOrDefault("imei", "");
                            if (imei.length() != 14 && imei.length() != 32 && !(imei.length() == 15 && StringUtil.pregMatch("([0-9]+)", imei))) {
                                imei = "";
                            }

                            r.put("imei", imei);

                            context.output(activityLogTag, gson.toJson(r));
                            break;

                        // 用户注册逻辑处理（PHP逻辑确认完成）
                        case "user":

                            if (r.get("uid").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            r.put("gender", !r.containsKey("gender") || !r.get("gender").equals("") ? "1" : "0");
                            r.put("update", !r.containsKey("update") || !r.get("update").equals("") ? "1" : "0");

                            context.output(userLogTag, gson.toJson(r));
                            break;


                        // 账号登录逻辑处理（PHP逻辑确认完成）
                        case "accountlogin":

                            if (r.get("uid").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            // TODO （PHP逻辑）单独从账号登录流分析处理：
                            //  1、更新设备激活保护期；
                            //  2。在线逻辑；


                            // 不做一日一次的登录记录，即全量记录
                            // eg, {"app_memory":"0","ip":"171.80.221.82","v_hour":"2019112310","avali_memory":"0","uid":"e0cc16d8b3ee825c9d049d5fdefd4ab9","city_name":"Wuhan","country_iso_code":"CN","v_week":"201947","tatol_memory":"0","v_month":"201911","v_day":"20191123","v_time":1574476474,"hd":"iPhone5S(GSM)","subdivision_iso_code":"HB","app_id":"20027","channel_id":"270001","did":"f940556f0ce265ae117b0085bd0631c7"}
                            context.output(accountLoginLogTag, gson.toJson(r));

                            break;

                        // 进入游戏逻辑处理（新）
                        case "entergame":

                            if (r.get("uid").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            context.output(enterGameLogTag, gson.toJson(r));
                            break;

                        // 游戏登录逻辑处理（PHP逻辑确认完成）
                        case "login":

                            if (r.get("pid").equals("") || r.get("uid").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            // TODO （PHP逻辑）单独从登录流分析处理：
                            //  1、在线逻辑；

                            r.put("logout", r.get("logout").equals("") ? "0" : "1");

                            context.output(loginLogTag, gson.toJson(r));
                            break;

                        // 创角事件逻辑处理（PHP逻辑确认完成）
                        case "player":

                            if (r.get("pid").equals("") || r.get("did").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            if (r.get("update").equals("") || r.get("update").equals("0")) {
                                context.output(playerLogTag, gson.toJson(r));
                            } else {
                                r.put("level", r.get("level").equals("") ? "1" : r.get("level"));
                                context.output(playerUpdateLogTag, gson.toJson(r));
                            }

                            break;

                        // 支付事件逻辑处理（PHP逻辑确认完成）
                        // TODO 屏蔽广告业务 & 根据角色获取设备渠道等信息在后续处理
                        case "pay":

                            if (r.get("pay_time").equals("") || r.get("order_id").equals("") || r.get("pid").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            r.put("player_level", r.get("player_level").equals("") ? "1" : r.get("player_level"));

                            context.output(payLogTag, gson.toJson(r));
                            break;

                        // 关卡事件逻辑处理（PHP逻辑确认完成）
                        case "mission":

                            // 关卡状态
                            String[] flags = new String[] {"begin", "complete", "failed"};

                            if (r.get("mission").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            String[] missions   = r.get("mission").split(",");
                            String[] servers    = r.get("sid").split(",");
                            String[] players    = r.get("pid").split(",");
                            String[] levels     = r.get("level").split(",");
                            String[] channels   = r.get("channel_id").split(",");
                            String[] times      = r.get("time").split(",");
                            String[] statuses   = r.get("status").split(",");
                            String[] firstPass  = r.get("firstpass").split(",");
                            String[] firstFail  = r.get("firstfail").split(",");
                            String[] firstEnter = r.get("firstenter").split(",");

                            // 默认时间
                            long mtime = times[0] != null ? Long.parseLong(times[0]) : requestDate.getTime();

                            // 默认级别
                            String level = levels[0] != null ? levels[0] : "1";

                            String[] items;
                            String status;
                            long itime;
                            Date idate;
                            String imonth;

                            Map<String, String> data = new HashMap<>();

                            int mc = missions.length;
                            for (int i = 0; i < mc; i++) {

                                // 关卡级别
                                items = missions[i].split("([_-])");

                                // 状态
                                status = statuses[i] != null ? statuses[i] : "";
                                if (!Arrays.asList(flags).contains(status)) {
                                    status = "";
                                }

                                // 时间处理
                                itime = levels[i] != null ? Long.parseLong(levels[i]) : mtime;
                                idate = new Date(itime * 1000);

                                // 事件上报月份
                                imonth = new SimpleDateFormat("yyyyMM").format(idate);
                                data.put("v_month", imonth);

                                calendar.setTime(requestDate);
                                int week = calendar.get(Calendar.WEEK_OF_YEAR);
                                if (week < 10) {
                                    data.put("v_week", String.format("%s0%d", imonth.substring(0, 4), week));
                                } else {
                                    data.put("v_week", String.format("%s%d", imonth.substring(0, 4), week));
                                }

                                // 事件上报日期
                                data.put("v_day", new SimpleDateFormat("yyyyMMdd").format(idate));

                                // 事件上报小时
                                data.put("v_hour", new SimpleDateFormat("yyyyMMddHH").format(idate));

                                data.put("sid", servers[i] != null ? servers[i] : servers[0]);
                                data.put("pid", players[i] != null ? players[i] : players[0]);
                                data.put("channel_id", channels[i] != null ? channels[i] : channels[0]);
                                data.put("level", levels[i] != null ? levels[i] : level);
                                data.put("v_time", String.format("%d", itime));
                                data.put("level_1", items.length >= 2 && items[1] != null ? items[1] : "0");
                                data.put("level_2", items.length >= 3 && items[2] != null ? items[2] : "0");
                                data.put("level_3", items.length >= 4 && items[3] != null ? items[3] : "0");
                                data.put("mission_id", items[0]);
                                data.put("status", status);
                                data.put("original_channel_id", r.get("original_channel_id"));
                                data.put("first_pass", firstPass[i] != null ? firstPass[i] : firstPass[0]);
                                data.put("first_fail", firstFail[i] != null ? firstFail[i] : firstFail[0]);
                                data.put("first_enter", firstEnter[i] != null ? firstEnter[i] : firstEnter[0]);

                                context.output(missionLogTag, gson.toJson(data));
                            }
                            break;

                        // 通用事件逻辑处理
                        // TODO 未处理
                        case "event":

                            context.output(eventLogTag, gson.toJson(r));
                            break;

                        // 应用记录逻辑处理
                        // TODO 未处理
                        case "record":

                            context.output(recordLogTag, gson.toJson(r));
                            break;

                        // 内购事件逻辑处理（PHP逻辑确认完成）
                        case "iap":

                            if (r.get("did").equals("") || r.get("oid").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                            } else {
                                context.output(iapLogTag, gson.toJson(r));
                            }

                            break;

                        // 调试事件逻辑处理（PHP逻辑确认完成）
                        case "sdkdebug":

                            int sdk = Integer.parseInt(r.get("sdk"));
                            if (!Arrays.asList(1,2,3,4,5,6,7,8,9).contains(sdk)) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            if (r.get("developer").equals("")) {
                                r.put("developer", "root");
                            }

                            context.output(clientDebugLogTag, gson.toJson(r));

                            break;

                        // 异常事件逻辑处理（PHP逻辑确认完成）
                        case "excepter":

                            if (r.get("did").equals("")) {
                                context.output(incompleteLogTag, new DateElement(requestDateString, element));
                                break;
                            }

                            context.output(exceptLogTag, gson.toJson(r));

                            break;

                        // 开放接口，不做处理
                        case "open":
                            context.output(openLogTag, new DateElement(requestDateString, element));
                            break;

                        // 未知接口，不做处理
                        default:
                            context.output(otherLogTag, new DateElement(requestDateString, element));
                            break;
                    }

                }
                else {
                    collector.collect(String.format("element don't match regex, %s", element));
                }
            } catch (Exception ex) {
                collector.collect(String.format("exception where process element, %s, %s", element, ex.toString()));
            }
        }
    }

    // 从数据看中定时获取配置产生一条数据源
    public static final class MySQLConfigSource extends RichSourceFunction<Tuple2<String, Map<String, String>>> implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final Logger log = LoggerFactory.getLogger(MySQLConfigSource.class);

        private String host;
        private Integer port;
        private String db;
        private String user;
        private String password;
        private Integer secondInterval;

        private volatile boolean isRunning = true;

        private Connection connection;

        public MySQLConfigSource(String host, Integer port, String db, String user, String password, Integer secondInterval) {
            this.host = host;
            this.port = port;
            this.db = db;
            this.user = user;
            this.password = password;
            this.secondInterval = secondInterval;
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

        @Override
        public void run(SourceContext<Tuple2<String, Map<String, String>>> context) {
            log.info("run custom source from mysql.");
            while (isRunning) {
                try {
                    // 从数据库中读取配置信息
                    Map<String, String> output = new HashMap<>();

                    // 查询应用配置
                    PreparedStatement preparedStatement = connection.prepareStatement("select app_id,app_key,game_status from xy_center.xy_game where etl_version = 2");
                    ResultSet resultSet = preparedStatement.executeQuery();

                    while (resultSet.next()){
                        Map<String, String> config = new HashMap<>();
                        config.put("appKey", resultSet.getString("app_key"));
                        config.put("appStatus", resultSet.getString("game_status"));
                        context.collect(new Tuple2<>(resultSet.getString("app_id"), config));
                    }

                    // 关闭
                    preparedStatement.close();

                    // 每隔多少秒执行一次查询
                    Thread.sleep(1000 * secondInterval);

                } catch (Exception ex){
                    log.error("query config from mysql is failed, error: ", ex);
                }
            }
        }

        @Override
        public void cancel() {
            log.info("cancel custom source from mysql.");
            isRunning = false;
        }
    }

    // 通过mysql查询语句从数据看中定时获取配置数据产生一批数据源
    public static final class MySQLConfigSourceFromFreeQuery extends RichSourceFunction<List<Map<String, String>>> implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final Logger log = LoggerFactory.getLogger(MySQLConfigSource.class);

        private String host;
        private Integer port;
        private String db;
        private String user;
        private String password;
        private Integer secondInterval;
        private String queryString;

        private volatile boolean isRunning = true;

        private Connection connection;

        public MySQLConfigSourceFromFreeQuery(String host, Integer port, String db, String user, String password, Integer secondInterval, String queryString) {
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

        @Override
        public void run(SourceContext<List<Map<String, String>>> context) {
            log.info("run custom source from mysql.");
            while (isRunning) {
                try {
                    List<Map<String, String>> list = new ArrayList<Map<String, String>>();
                    // 查询应用配置
                    PreparedStatement preparedStatement = connection.prepareStatement(queryString);
                    ResultSet resultSet = preparedStatement.executeQuery();

                    ResultSetMetaData md = resultSet.getMetaData(); //获得结果集结构信息,元数据
                    int columnCount = md.getColumnCount();   //获得列数
                    while (resultSet.next()) {
                        Map<String, String> rowData = new HashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            rowData.put(md.getColumnName(i), resultSet.getString(i));
                        }
                        list.add(rowData);
                    }

                    context.collect(list);

                    // 关闭
                    preparedStatement.close();

                    // 每隔多少秒执行一次查询
                    Thread.sleep(1000 * secondInterval);

                } catch (Exception ex){
                    log.error("query config from mysql is failed, error: ", ex);
                }
            }
        }

        @Override
        public void cancel() {
            log.info("cancel custom source from mysql.");
            isRunning = false;
        }
    }

    // 定义一个带有日期的原始日志类
    @ToString
    @AllArgsConstructor
    private static final class DateElement implements Serializable {
        private static final long serialVersionUID = 1L;
        @Getter @Setter private String date;
        @Getter @Setter private String element;
    }

    // 获取日志本身的请求时间保持跟原始请求日志分目录一致
    public static final class DateBucketAssigner implements BucketAssigner<DateElement, String> {
        private static final long serialVersionUID = 1L;

        public String getBucketId(DateElement element, Context context) {
            return element.date;
        }

        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
