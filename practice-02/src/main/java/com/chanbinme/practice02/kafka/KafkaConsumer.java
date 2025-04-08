package com.chanbinme.practice02.kafka;

import com.chanbinme.practice02.kafka.dto.DemoViewDTO1;
import com.chanbinme.practice02.kafka.dto.DemoViewDTO2;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Data
@KafkaListener(groupId = "chanbinme-groupId", topics = "chanbinme-topic")
public class KafkaConsumer {

    private CountDownLatch latch = new CountDownLatch(10);
    private List<Object> payloads = new ArrayList<>();
    private Object payload;

    @KafkaHandler
    public void handleDTO1(DemoViewDTO1 dto) {
        log.info("Received DTO1: {}", dto);
        payloads.add(dto);
        latch.countDown();
    }

    @KafkaHandler
    public void handleDTO2(DemoViewDTO2 dto) {
        log.info("Received DTO2: {}", dto);
        payloads.add(dto);
        latch.countDown();
    }

    @KafkaHandler(isDefault = true)
    public void handleDefault(Object dto) {
        log.info("Received unknown type: {}", dto);
        payloads.add(dto);
        latch.countDown();
    }
}
