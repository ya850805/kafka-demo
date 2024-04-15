package org.example.kafka.test2024.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jason
 * @description
 * @create 2024/4/13 02:33
 **/
public class KafkaProducerTest {
    public static void main(String[] args) {
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

        /**
         * 創建生產者對象
         *      生產者對象需要設定泛型：數據的類型約束
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configMap);

        /**
         * 創建數據
         *      構建數據時需要傳遞3個參數
         *      1. 主題的名稱：指定的主題如果不存在，會自動創建
         *      2. 數據的key
         *      3. 數據的value：真正的數據
         */
//        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "key", "value");

        /**
         * 通過生產者對象將數據發送到Kafka
         */
//        producer.send(record);

        //多發送幾條
        for(int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "key" + i, "value" + i);
            producer.send(record);
        }

        /**
         * 關閉生產者對象
         */
        producer.close();
    }
}
