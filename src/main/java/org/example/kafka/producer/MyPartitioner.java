package org.example.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author jason
 * @description
 * @create 2024/1/10 14:54
 **/
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //獲取數據
        String msgValues = value.toString();

        //包含某個字符串，就發到0號分區，其餘的發到1號分區
        int partitions;
        if(msgValues.contains("hello")) {
            partitions = 0;
        } else {
            partitions = 1;
        }

        return partitions;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
