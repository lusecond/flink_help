package com.feiyu.thread_switching;

import com.alibaba.fastjson.JSONObject;
import com.feiyu.test4.DBConnectUtil;
import com.feiyu.test4.MysqlExactlyOncePOJO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class JDBCThreadSwitching {

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

                    try {
                        //connect
                        Callable<Connection> callable  =new connect();
                        FutureTask <Connection>futureTask=new FutureTask<>(callable);
                        Thread mThread=new Thread(futureTask);
                        mThread.start();
                        Connection connection = futureTask.get();

                        //设置手动提交
                        connection.setAutoCommit(false);

                        //insert
                        Callable<Connection> callable2  =new insertBatch(connection, list);
                        FutureTask <Connection>futureTask2=new FutureTask<>(callable2);
                        Thread mThread2=new Thread(futureTask2);
                        mThread2.start();
                        Connection connection2 = futureTask2.get();

                        //commit
                        Callable<Connection> callable3  =new commit(connection2);
                        FutureTask <Connection>futureTask3=new FutureTask<>(callable3);
                        Thread mThread3=new Thread(futureTask3);
                        mThread3.start();
                        Connection connection3 = futureTask3.get();
//                        Connection connection3 = (new commit(connection2)).call();

                        //close
                        Callable<Integer> callable4  =new close(connection3);
                        FutureTask <Integer>futureTask4=new FutureTask<>(callable4);
                        Thread mThread4=new Thread(futureTask4);
                        mThread4.start();
//                        (new close(connection3)).call();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }, 5000L ,5000L);

    }

}
