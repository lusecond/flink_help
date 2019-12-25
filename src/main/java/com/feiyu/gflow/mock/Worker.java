package com.feiyu.gflow.mock;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class Worker {
    private static final String toKafkaBootstrapServers = "10.250.0.101:9092,10.250.0.102:9092,10.250.0.103:9092";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new LogMockSource()).addSink(new FlinkKafkaProducer011<>(toKafkaBootstrapServers, "log-mock", new SimpleStringSchema()));
        env.execute(Worker.class.getName());
    }
}
