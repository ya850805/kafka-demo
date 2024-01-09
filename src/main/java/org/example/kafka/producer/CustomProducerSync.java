package org.example.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author jason
 * @description
 * @create 2024/1/9 18:18
 **/
public class CustomProducerSync {
    //同步方式把消息發送到kafka集群，同步發送需要等待消息真正送到broker中，才會發送下一批消息
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //0. 配置
        Properties properties = new Properties();

        //連接集群 bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //指定對應的key和value的序列化類型 key.serializer / value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //1. 創建kafka生產者對象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2. 同步發送數據
        for (int i = 0; i < 5; i++) {
            //添加get()底層就是同步發送
            kafkaProducer.send(new ProducerRecord<>("first", "@@@value@@@" + i)).get();
        }

        //3. 關閉資源
        kafkaProducer.close();
    }
}
