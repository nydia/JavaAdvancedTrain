package com.nydia.delay.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class StreamDelayProducer {

    private final StreamDelayProperties properties;
    private final RabbitTemplate rabbitTemplate;

    public String sendDelayMessage(String payload, long delay, TimeUnit unit) {
        String taskId = UUID.randomUUID().toString();
        return sendDelayMessage(taskId, payload, delay, unit);
    }

    public String sendDelayMessage(String taskId, String payload, long delay, TimeUnit unit) {
        long delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);

        Map<String, Object> messageBody = new HashMap<>();
        messageBody.put("taskId", taskId);
        messageBody.put("payload", payload);
        messageBody.put("delay", delayMillis);
        messageBody.put("submitTime", System.currentTimeMillis());

        MessagePostProcessor processor = message -> {
            message.getMessageProperties().setExpiration(String.valueOf(delayMillis));
            return message;
        };

        rabbitTemplate.convertAndSend(
                properties.getExchange(),
                properties.getRoutingKey(),
                messageBody,
                processor
        );

        log.info("RabbitMQ delay message sent: taskId={}, delay={}ms", taskId, delayMillis);
        return taskId;
    }
}
