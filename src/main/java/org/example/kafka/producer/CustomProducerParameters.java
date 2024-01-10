package org.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/10 15:04
 **/
public class CustomProducerParameters {
    public static void main(String[] args) {
        //0. 配置信息
        Properties properties = new Properties();

        //連接kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 提高生產者吞吐量
         * 1. 緩衝區大小
         * 2. 批次大小
         * 3. linger.ms
         * 4. 壓縮
         */
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); //緩衝區大小32M
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); //批次大小16k
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1); //1毫秒拉取一次
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //壓縮類型snappy

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
