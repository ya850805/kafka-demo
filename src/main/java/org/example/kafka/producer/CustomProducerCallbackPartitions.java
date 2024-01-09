package org.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/9 18:18
 **/
public class CustomProducerCallbackPartitions {
    //異步方式把消息發送到kafka集群，並指定Callback
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

        //2. 發送數據並指定Callback
        for (int i = 0; i < 5; i++) {
            /**
             * 1. 直接指定partition
             * 2. 指定key -> 會取得key的hash值，然後使用該hash值對整體的partition數量取餘數，得到一個分區號並發往該分區
             * 3. 沒有指定partition也沒有指定key，會使用黏性分區器，會隨機使用一個分區，並盡可能一直使用該分區，直到該分區batch已滿或達到linger.ms，才會再隨機選擇一個分區(不會與上一個分區相同，如果相同則繼續隨機)
             */
            kafkaProducer.send(new ProducerRecord<>("first", 0, "",  "@@@value@@@" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(null == e) {
                        System.out.println("主題：" + recordMetadata.topic() + " 分區：" + recordMetadata.partition());
                    }
                }
            });
        }

        //3. 關閉資源
        kafkaProducer.close();
    }
}
