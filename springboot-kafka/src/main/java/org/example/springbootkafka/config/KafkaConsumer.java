package org.example.springbootkafka.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * @author jason
 * @description
 * @create 2024/1/22 18:48
 **/
@Configuration
public class KafkaConsumer {

    @KafkaListener(topics = "xxxxx")
    public void consumeTopic(String msg) {
        System.out.println("收到消息：" + msg);
    }
}
