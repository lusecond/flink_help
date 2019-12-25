package com.feiyu.thread_switching;

import com.alibaba.fastjson.JSON;
import com.feiyu.test4.MysqlExactlyOncePOJO;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaUtils {
    //    private static final String broker_list = "localhost:9092";
    private static final String broker_list = "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092";
    //flink 读取kafka写入mysql exactly-once 的topic
    private static final String topic_ExactlyOnce = "mysql-exactly-Once-4";

    public static void writeToKafka2() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        KafkaProducer producer = new KafkaProducer<String, String>(props);//老版本producer已废弃
        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        try {
            for (int i = 1; i <= 10000; i++) {
                MysqlExactlyOncePOJO mysqlExactlyOnce = new MysqlExactlyOncePOJO(String.valueOf(i));
                ProducerRecord record = new ProducerRecord<String, String>(topic_ExactlyOnce, null, null, JSON.toJSONString(mysqlExactlyOnce));
                producer.send(record);
                System.out.println("发送数据: " + JSON.toJSONString(mysqlExactlyOnce));
                Thread.sleep(10);
            }
        } catch (Exception e){

        }

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        writeToKafka2();
    }
}

