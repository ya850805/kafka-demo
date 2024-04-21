package org.example.kafka.test2024.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jason
 * @description
 * @create 2024/4/13 02:46
 **/
public class KafkaConsumerGroup2Test {
    public static void main(String[] args) {
        /**
         * 創建配置對象
         */
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        /**
         * 反序列化數據
         */
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "atguigu");


        /**
         * 創建消費者對象
         */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configMap);

        /**
         * 訂閱主題
         */
        consumer.subscribe(Collections.singletonList("test"));

        /**
         * 從Kafka主題中獲取數據
         *      消費者從Kafka中"拉取"數據
         */
        while(true) { //不間斷的拉取數據
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //拉取"一批"數據
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.partition());
            }
        }


        /**
         * 關閉消費者對象
         */
//        consumer.close();
    }
}
