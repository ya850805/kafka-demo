package org.example.kafka.test2024.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author jason
 * @description
 * @create 2024/4/13 02:33
 **/
public class KafkaProducerCallbackTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        /**
         * 創建配置對象
         */
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        /**
         * 對生產的數據K, V進行序列化的操作
         */
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.ACKS_CONFIG, "-1"); //ack

        /**
         * 創建生產者對象
         *      生產者對象需要設定泛型：數據的類型約束
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configMap);

        /**
         * 通過生產者對象將數據發送到Kafka
         */
        for(int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "key" + i, "value" + i);
            //1. 異步發送
//            producer.send(record, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    System.out.println("數據發送成功" + recordMetadata);
//                }
//            });
//            System.out.println("發送數據");

            //2. 同步發送
            Future<RecordMetadata> send = producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("數據發送成功" + recordMetadata);
                }
            });
            System.out.println("發送數據");
            send.get();
        }

        /**
         * 關閉生產者對象
         */
        producer.close();
    }
}
