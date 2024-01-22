package org.example.springbootkafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author jason
 * @description
 * @create 2024/1/22 18:41
 **/
@RestController
public class ProducerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/send")
    public String data(String msg) {
        //通過kafka發送出去
        kafkaTemplate.send("xxxxx", msg);
        return "ok";
    }
}
