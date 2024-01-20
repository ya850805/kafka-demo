package org.example.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author jason
 * @description
 * @create 2024/1/20 20:36
 **/
public class FlinkKafkaProducer1 {
    public static void main(String[] args) throws Exception {
        //1. 獲取環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //2. 準備數據源
        ArrayList<String> wordList = new ArrayList<>();
        wordList.add("hello");
        wordList.add("xxx");
        DataStreamSource<String> stream = env.fromCollection(wordList);

        //創建一個kafka生產者
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("first", new SimpleStringSchema(), properties);

        //3. 添加數據源 輸入到kafka生產者
        stream.addSink(kafkaProducer);

        //4. 執行代碼
        env.execute();
    }
}
