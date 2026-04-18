package com.nydia.delay.rabbitmq;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;
import java.io.IOException;

@Service
@Slf4j
public class DelayConsumer {

    /**
     * 监听延迟队列
     */
    @RabbitListener(queues = RabbitDelayConfig.DELAY_QUEUE)
    public void receiveDelayMsg(String msg, Channel channel, Message message) throws IOException {
        try {
            log.info("【延迟消息已消费】：{}", msg);

            // 业务处理 ==================
            doBusiness(msg);

            // 手动ACK，确认消息
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);

        } catch (Exception e) {
            log.error("消费异常", e);
            // 异常：拒绝消息
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
        }
    }

    private void doBusiness(String msg) {
        // 你的业务：订单超时、取消、关闭等
    }
}