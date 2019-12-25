package com.feiyu.test4;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class JDBCTest {

    public static void main(String[] args){

        String url = "jdbc:mysql://10.250.0.38:3306/xy_data_20027?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true&rewriteBatchedStatements=true";
//        final Connection connection = null;

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "duanjt_test");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        kafkaConsumer.subscribe(Collections.singletonList("mysql-exactly-Once-4"));// 订阅消息

        Timer time = new Timer();
        time.schedule(new TimerTask() {
            @Override
            public void run() {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
                System.out.println("本次获取数量：" + records.count());
                if (records.count() != 0) {
                    List<MysqlExactlyOncePOJO> list = new ArrayList<MysqlExactlyOncePOJO>();

                    for (ConsumerRecord<String, String> record : records) {
                        JSONObject valueJson = JSONObject.parseObject(record.value());
                        String value_str = (String) valueJson.get("value");
                        list.add(new MysqlExactlyOncePOJO(String.valueOf(value_str)));
                    }
                    String sql = "insert into `mysqlExactlyOnce_test` (`value`,`insert_time`) values (?,?)";
                    Connection connection = null;
                    try {
                        connection = DBConnectUtil.getConnection(url, "public", "public");
                        //设置手动提交
                        connection.setAutoCommit(false);
                        PreparedStatement ps = connection.prepareStatement(sql);
                        for (MysqlExactlyOncePOJO t:list) {
                            ps.setObject(1,t.getValue());
//                            ps.setString(1,t.getValue());
                            Timestamp value_time = new Timestamp(System.currentTimeMillis());
                            ps.setObject(2,value_time);
//                            ps.setTimestamp(2,value_time);
                            ps.addBatch();
                        }
                        //执行insert语句
                        ps.executeBatch();
                        ps.close();
                        DBConnectUtil.commit(connection);
                    } catch (Exception e) {
                        if (connection != null) {
                            try {
                                connection.rollback();
                            } catch (SQLException ee) {
                                System.out.println(ee.toString());
                            }
                        }
                        e.printStackTrace();
                    }
                }

            }
        }, 5000L ,5000L);

    }

}
