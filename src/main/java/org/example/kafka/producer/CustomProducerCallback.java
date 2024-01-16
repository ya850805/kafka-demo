package org.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/9 18:18
 **/
public class CustomProducerCallback {
    //異步方式把消息發送到kafka集群，並指定Callback
    public static void main(String[] args) throws InterruptedException {
        //0. 配置
        Properties properties = new Properties();

        //連接集群 bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //指定對應的key和value的序列化類型 key.serializer / value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //1. 創建kafka生產者對象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2. 發送數據並指定Callback
        for (int i = 0; i < 500; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "@@@value@@@" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(null == e) {
                        System.out.println("主題：" + recordMetadata.topic() + " 分區：" + recordMetadata.partition());
                    }
                }
            });

            Thread.sleep(1);
        }

        //3. 關閉資源
        kafkaProducer.close();
    }
}
