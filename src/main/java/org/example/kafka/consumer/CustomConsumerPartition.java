package org.example.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/16 21:47
 **/
public class CustomConsumerPartition {
    public static void main(String[] args) {
        //0. 配置
        Properties properties = new Properties();

        //連接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //組id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        //1. 創建一個消費者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //2. 訂閱主題對應的分區
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition("first", 0));
        consumer.assign(topicPartitions);

        //3. 消費數據
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
