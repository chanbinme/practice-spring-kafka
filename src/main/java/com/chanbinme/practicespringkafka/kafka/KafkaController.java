package com.chanbinme.practicespringkafka.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaProducerService kafkaProducerService;

    @PostMapping()
    public String sendMessage(@RequestParam("message") String message) {
        kafkaProducerService.sendMessage(message);

        return "Message sent to Kafka topic";
    }

}
