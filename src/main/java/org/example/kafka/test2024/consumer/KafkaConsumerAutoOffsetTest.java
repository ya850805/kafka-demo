package org.example.kafka.test2024.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author jason
 * @description
 * @create 2024/4/13 02:46
 **/
public class KafkaConsumerAutoOffsetTest {
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

        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "atguigu2");
        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); //關閉自動提交(消費數據時需要手動提交(異步/同步))
//        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //從最早的偏移量開始消費

        /**
         * 創建消費者對象
         */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configMap);

        /**
         * 訂閱主題
         */
        consumer.subscribe(Collections.singletonList("test1"));

        /**
         * 獲取集群信息
         */
        boolean flag = true;
        while(flag) {
            consumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> assignment = consumer.assignment(); //獲取當前消費主題的分區信息
            if(assignment != null && !assignment.isEmpty()) { //有拉取到數據
                for (TopicPartition topicPartition : assignment) {
                    if("test1".equals(topicPartition.topic())) {
                        consumer.seek(topicPartition, 2); //這個分區從偏移量2的地方開始消費
                        flag = false;
                    }
                }
            }
        }

        /**
         * 從Kafka主題中獲取數據
         *      消費者從Kafka中"拉取"數據
         */
        while(true) { //不間斷的拉取數據
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //拉取"一批"數據
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
            /**
             * 手動保存偏移量
             */
            consumer.commitAsync(); //異步提交偏移量(較常使用)
//            consumer.commitSync(); //同步提交偏移量
        }


        /**
         * 關閉消費者對象
         */
//        consumer.close();
    }
}
