package com.nydia.delay.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "spring.cloud.stream.default-binder", havingValue = "rocket")
public class StreamDelayRocketConfig {

    private static final String PRODUCER_GROUP = "delay-producer-group";

    @Bean
    public DefaultMQProducer defaultMQProducer(StreamDelayProperties properties) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        String nameServer = properties.getRocketmq() != null && properties.getRocketmq().getNameServer() != null
                ? properties.getRocketmq().getNameServer()
                : properties.getNameServer();
        producer.setNamesrvAddr(nameServer);
        producer.setInstanceName("delay-producer");
        producer.setMaxMessageSize(1024 * 1024 * 4);
        return producer;
    }

    @Bean
    @Primary
    public DefaultMQProducer startProducer(DefaultMQProducer producer) {
        try {
            producer.start();
            log.info("RocketMQ Producer started successfully");
        } catch (Exception e) {
            log.error("Failed to start RocketMQ Producer", e);
            throw new RuntimeException("Failed to start RocketMQ Producer", e);
        }
        return producer;
    }
}