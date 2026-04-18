package com.nydia.delay.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.cloud.stream.default-binder", havingValue = "rocket")
public class StreamDelayRocketConsumer {

    private final StreamDelayProperties properties;

    @jakarta.annotation.PostConstruct
    public void init() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(properties.getGroup());
        consumer.setNamesrvAddr(properties.getNameServer());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(properties.getTopic(), properties.getTag());
        consumer.registerMessageListener((MessageListenerConcurrently) (msgList, context) -> {
            for (org.apache.rocketmq.common.message.MessageExt msg : msgList) {
                try {
                    String body = new String(msg.getBody());
                    log.info("RocketMQ received delay message: {}", body);
                } catch (Exception e) {
                    log.error("Failed to process message", e);
                }
            }
            return org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        log.info("RocketMQ consumer started: group={}, topic={}", properties.getGroup(), properties.getTopic());
    }
}