package org.example.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @author jason
 * @description
 * @create 2024/1/17 17:59
 **/
public class CustomConsumerSeekTime {
    public static void main(String[] args) {
        //0. 配置信息
        Properties properties = new Properties();

        //連接
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //組id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testxxxx");

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

        //希望把時間轉換為對應的offset
        HashMap<TopicPartition, Long> topicPartitionLongHashMap = new HashMap<>();
        //封裝對應集合
        for (TopicPartition topicPartition : assignment) {
            //當前時刻一天前offset的位置 {分區, 前一天的時間(ms)}
            topicPartitionLongHashMap.put(topicPartition, System.currentTimeMillis() - 2 * 24 * 3600 * 1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = consumer.offsetsForTimes(topicPartitionLongHashMap);

        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
            consumer.seek(topicPartition, offsetAndTimestamp.offset());
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
