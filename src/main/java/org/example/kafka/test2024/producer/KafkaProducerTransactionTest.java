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
public class KafkaProducerTransactionTest {
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
//        configMap.put(ProducerConfig.ACKS_CONFIG, "-1");
        configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); //開啟冪等性，此時ACK就會是-1，並且需要開啟重試
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        configMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        configMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-tx-id"); //事務ID，事務是基於冪等性操作的

        /**
         * 創建生產者對象
         *      生產者對象需要設定泛型：數據的類型約束
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configMap);

        /**
         * 初始化事務
         */
        producer.initTransactions();

        try {

            /**
             * 開啟事務
             */
            producer.beginTransaction();

            /**
             * 通過生產者對象將數據發送到Kafka
             */
            for(int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", "key" + i, "value" + i);
                producer.send(record);
            }

            /**
             * 提交事務
             */
            producer.commitTransaction();
        } catch (Exception e) {
            /**
             * 捕獲異常，終止事務
             */
            producer.abortTransaction();
        } finally {
            /**
             * 關閉生產者對象
             */
            producer.close();
        }

    }
}
