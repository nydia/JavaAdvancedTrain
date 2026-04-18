package com.nydia.delay.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.cloud.stream.default-binder", havingValue = "rocket")
public class StreamDelayRocketProducer {

    private final StreamDelayProperties properties;
    private final DefaultMQProducer rocketMQProducer;

    public String sendDelayMessage(String payload, long delay, TimeUnit unit) {
        String taskId = UUID.randomUUID().toString();
        return sendDelayMessage(taskId, payload, delay, unit);
    }

    public String sendDelayMessage(String taskId, String payload, long delay, TimeUnit unit) {
        long delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);

        int delayLevel = calculateDelayLevel(delayMillis);

        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("taskId", taskId);
        messageBody.put("payload", payload);
        messageBody.put("delay", delayMillis);
        messageBody.put("submitTime", System.currentTimeMillis());

        try {
            String jsonBody = com.alibaba.fastjson.JSON.toJSONString(messageBody);
            org.apache.rocketmq.common.message.Message message = new org.apache.rocketmq.common.message.Message(
                    properties.getTopic(),
                    properties.getTag(),
                    taskId,
                    jsonBody.getBytes()
            );
            message.setDelayTimeLevel(delayLevel);

            rocketMQProducer.send(message);
            log.info("RocketMQ delay message sent: taskId={}, delay={}ms, delayLevel={}", taskId, delayMillis, delayLevel);
        } catch (Exception e) {
            log.error("Failed to send RocketMQ delay message", e);
            throw new RuntimeException("Failed to send delay message", e);
        }

        return taskId;
    }

    private int calculateDelayLevel(long delayMillis) {
        if (delayMillis <= 5000) return 1;
        if (delayMillis <= 10000) return 2;
        if (delayMillis <= 30000) return 3;
        if (delayMillis <= 60000) return 4;
        if (delayMillis <= 120000) return 5;
        if (delayMillis <= 180000) return 6;
        if (delayMillis <= 300000) return 7;
        if (delayMillis <= 600000) return 8;
        if (delayMillis <= 1800000) return 9;
        return 10;
    }
}
