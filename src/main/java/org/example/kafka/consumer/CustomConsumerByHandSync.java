package org.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/16 21:34
 **/
public class CustomConsumerByHandSync {
    public static void main(String[] args) {
        //0. 配置
        Properties properties = new Properties();

        //連接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //消費者組id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test7");

        //關閉自動提交 => 手動提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        //1. 創建一個消費者
        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties);

        //2. 訂閱主題
        ArrayList<Object> topics = new ArrayList<>();
        topics.add("first");
        consumer.subscribe(topics);

        //3. 消費數據
        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

            //手動提交offset(同步)
            consumer.commitSync();
        }
    }
}
