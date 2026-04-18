package com.nydia.delay.rabbitmq;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitDelayConfig {

    // 交换机名称
    public static final String DELAY_EXCHANGE = "delay.exchange";
    // 队列名称
    public static final String DELAY_QUEUE = "delay.queue";
    // routingKey
    public static final String DELAY_ROUTING_KEY = "delay.routingKey";

    /**
     * 1. 创建【延迟交换机】
     * 类型：x-delayed-message
     */
    @Bean
    public CustomExchange delayExchange() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        return new CustomExchange(DELAY_EXCHANGE, "x-delayed-message", true, false, args);
    }

    /**
     * 2. 创建延迟队列
     */
    @Bean
    public Queue delayQueue() {
        return QueueBuilder.durable(DELAY_QUEUE).build();
    }

    /**
     * 3. 绑定
     */
    @Bean
    public Binding delayBinding(Queue delayQueue, CustomExchange delayExchange) {
        return BindingBuilder.bind(delayQueue).to(delayExchange).with(DELAY_ROUTING_KEY).noargs();
    }
}