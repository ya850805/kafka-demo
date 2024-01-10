package org.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/10 15:29
 **/
public class CustomProducerAcks {
    public static void main(String[] args) {
        //0. 配置信息
        Properties properties = new Properties();

        //連接kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //acks
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        //重試次數
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        //1. 創建生產者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //2. 發送數據
        for(int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "~~message~~~" + i));
        }

        //3. 關閉資源
        producer.close();
    }
}
