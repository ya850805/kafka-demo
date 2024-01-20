package org.example.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/20 20:47
 **/
public class FlinkKafkaConsumer1 {
    public static void main(String[] args) throws Exception {
        // 0 初始化 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 1 kafka 消费者配置信息
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");

        // 2 创建 kafka 消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("first",new SimpleStringSchema(), properties);

        // 3 消费者和 flink 流关联
        env.addSource(kafkaConsumer).print();

        // 4 执行
        env.execute();
    }
}
