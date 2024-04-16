package org.example.kafka.test2024.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author jason
 * @description
 * @create 2024/4/15 23:33
 *
 *  自定義攔截器
 *  1. 實現ProducerInterceptor
 *  2. 定義泛型
 *  3. 重寫方法
 **/
public class ValueInterceptorTest implements ProducerInterceptor<String, String> {
    /**
     * 發送數據時，會調用該方法
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        //把value重複兩次
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.key(), producerRecord.value() + producerRecord.value());
    }

    /**
     * 發送數據完畢，服務器返回的響應，會調用此方法
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    /**
     * 生產者對象關閉時，會調用此方法(釋放資源)
     */
    @Override
    public void close() {

    }

    /**
     * 創建生產者對象時調用
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
