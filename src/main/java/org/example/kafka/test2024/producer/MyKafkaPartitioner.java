package org.example.kafka.test2024.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author jason
 * @description
 * @create 2024/4/16 23:10
 *
 *  自定義分區器
 *  1. 實現Partitioner接口
 *  2. 重寫方法
 **/
public class MyKafkaPartitioner implements Partitioner {
    /**
     *  根據參數自定義分區規則
     * @param s
     * @param o
     * @param bytes
     * @param o1
     * @param bytes1
     * @param cluster
     * @return
     */
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
