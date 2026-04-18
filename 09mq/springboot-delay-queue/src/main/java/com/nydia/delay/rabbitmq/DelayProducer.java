package com.nydia.delay.rabbitmq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;

@Service
@Slf4j
public class DelayProducer {

    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * 发送延迟消息
     * @param msg 消息内容
     * @param delayMillis 延迟时间（毫秒）
     */
    public void sendDelayMsg(String msg, long delayMillis) {
        log.info("发送延迟消息：{}，延迟：{}ms", msg, delayMillis);

        rabbitTemplate.convertAndSend(
                RabbitDelayConfig.DELAY_EXCHANGE,
                RabbitDelayConfig.DELAY_ROUTING_KEY,
                msg,
                message -> {
                    // 设置延迟时间（关键）
                    message.getMessageProperties().setHeader("x-delay", delayMillis);
                    return message;
                }
        );
    }
}