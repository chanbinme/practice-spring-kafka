package com.chanbinme.practicespringkafka.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    @KafkaListener(topics = "chanbinme-topic")
    public void listen(@Payload String message) {
        System.out.println("Received message: " + message);
    }

    @KafkaListener(topics = "chanbinme-topic", groupId = "chanbinme-groupId")
    public void listen(@Payload String message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println("Received message: " + message + " from partition: " + partition);
    }

    @KafkaListener(topics = "chanbinme-topic", containerFactory = "filteredKafkaListenerContainerFactory")
    public void filteredListen(@Payload String message) {
        System.out.println("Filtered message: " + message);
    }
}
