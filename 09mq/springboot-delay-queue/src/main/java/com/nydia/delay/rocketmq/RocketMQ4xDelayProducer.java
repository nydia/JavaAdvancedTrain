package com.nydia.delay.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * RocketMQ 4.x 固定延迟队列 - 生产者
 * 延迟等级：1=1s、2=5s、3=10s、4=30s、5=1m...18=2h（可通过Broker配置修改）
 */
@Slf4j
public class RocketMQ4xDelayProducer {
    // 1. 核心配置（需根据自身环境修改）
    private static final String PRODUCER_GROUP = "delay_producer_group_4x";
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "delay_topic_4x";
    private static final String TAGS = "delay_tag_4x"; // 消息标签，用于消费者过滤

    public static void main(String[] args) {
        // 2. 初始化生产者
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        // 设置NameServer地址
        producer.setNamesrvAddr(NAMESRV_ADDR);
        // 可选配置：设置消息重试次数（默认2次）
        producer.setRetryTimesWhenSendFailed(3);

        try {
            // 3. 启动生产者（必须启动，否则无法发送消息）
            producer.start();
            log.info("RocketMQ 4.x 延迟队列生产者启动成功！");

            // 4. 循环发送10条延迟消息（模拟实际业务场景）
            for (int i = 1; i <= 10; i++) {
                // 构建消息：Topic、Tags、消息内容
                String messageContent = "RocketMQ 4.x 固定延迟消息 - " + i + "（延迟等级3，10s后投递）";
                Message message = new Message(TOPIC, TAGS, messageContent.getBytes());

                // 设置延迟等级（3级=10s延迟，可修改为1-18级）
                message.setDelayTimeLevel(3);

                // 5. 发送消息（同步发送，确保消息发送成功）
                SendResult sendResult = producer.send(message);
                log.info("延迟消息发送成功，消息ID：{}，延迟等级：3，预计投递时间：10s后", sendResult.getMsgId());

                // 间隔1s发送一条，避免消息发送过快
                Thread.sleep(1000);
            }
        } catch (MQClientException e) {
            log.error("RocketMQ 4.x 生产者启动失败，原因：{}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("RocketMQ 4.x 消息发送失败，原因：{}", e.getMessage(), e);
        } finally {
            // 6. 关闭生产者（释放资源，避免内存泄漏）
            if (producer != null) {
                producer.shutdown();
                log.info("RocketMQ 4.x 延迟队列生产者关闭成功！");
            }
        }
    }
}