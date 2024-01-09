package org.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/9 18:18
 **/
public class CustomProducer {
    //異步方式把消息發送到kafka集群
    public static void main(String[] args) {
        //0. 配置
        Properties properties = new Properties();

        //連接集群 bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //指定對應的key和value的序列化類型 key.serializer / value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //1. 創建kafka生產者對象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2. 發送數據
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "@@@value@@@" + i));
        }

        //3. 關閉資源
        kafkaProducer.close();
    }
}
