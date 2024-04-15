package org.example.kafka.test2024.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jason
 * @description
 * @create 2024/4/15 19:04
 **/
public class AdminTopicTest {
    public static void main(String[] args) {
        Map<String, Object> confMap = new HashMap<>();
        confMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        /**
         * 創建管理員對象
         */
        Admin admin = Admin.create(confMap);

        /**
         * 構建主題時需要傳遞3個參數
         *      1. 主題名稱：英文字母、數字、點、下滑線、中橫線(不推薦點和下滑線同時使用)
         *      2. 分區數量：int
         *      3. 主題分區副本因子(數量)：short
         */
        String topicName = "test1";
        int partitionCount = 1;
        short replicationCount = 1;
        NewTopic topic1 = new NewTopic(topicName, partitionCount, replicationCount);

        String topicName1 = "test2";
        int partitionCount1 = 2;
        short replicationCount1 = 2;
        NewTopic topic2 = new NewTopic(topicName1, partitionCount1, replicationCount1);

        String topicName2 = "test4";
        Map<Integer, List<Integer>> map = new HashMap<>(); //自定義主題分區副本策略
        map.put(0, Arrays.asList(3, 1)); //0號分區，放在broker3和broker1，寫在前面的broker就會是leader
        map.put(1, Arrays.asList(2, 3)); //1號分區，放在broker2和broker3，寫在前面的broker就會是leader
        map.put(2, Arrays.asList(1, 2)); //2號分區，放在broker1和broker2，寫在前面的broker就會是leader
        NewTopic topic3 = new NewTopic(topicName2, map);

        /**
         * 創建主題
         */
        CreateTopicsResult result = admin.createTopics(Arrays.asList(topic3));

        /**
         * In - Sync - Replicas: 同步副本列表(ISR)
         */

        /**
         * 關閉管理者對象
         */
        admin.close();
    }
}
