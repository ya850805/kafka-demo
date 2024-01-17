package org.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author jason
 * @description
 * @create 2024/1/17 17:59
 **/
public class CustomConsumerSeek {
    public static void main(String[] args) {
        //0. 配置信息
        Properties properties = new Properties();

        //連接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //組id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        //1. 創建消費者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //2. 訂閱主題
        List<String> topics = new ArrayList<>();
        topics.add("first");
        consumer.subscribe(topics);

        //指定位置進行消費
        Set<TopicPartition> assignment = consumer.assignment(); //取得分區信息

        //保證分區分配方案已經制定完畢
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            assignment = consumer.assignment();
        }

        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition, 100); //offset設置成100
        }

        //3. 消費數據
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
