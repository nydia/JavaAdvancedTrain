package com.nydia.delay.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.cloud.stream.default-binder", havingValue = "rabbit", matchIfMissing = true)
public class StreamDelayConsumer {

    @RabbitListener(queues = "delay-queue")
    public void handleDelayMessage(Map<String, Object> message) {
        String taskId = (String) message.get("taskId");
        String payload = (String) message.get("payload");
        Long delay = (Long) message.get("delay");
        Long submitTime = (Long) message.get("submitTime");

        log.info("RabbitMQ received delay message: taskId={}, payload={}, delay={}ms, submitTime={}",
                taskId, payload, delay, submitTime);

        try {
            processMessage(taskId, payload);
        } catch (Exception e) {
            log.error("Failed to process delay message: {}", taskId, e);
            throw e;
        }
    }

    private void processMessage(String taskId, String payload) {
        log.info("Processing delay task: taskId={}, payload={}", taskId, payload);
    }
}