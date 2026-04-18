package com.nydia.delay.rocketmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * RocketMQ 4.x 固定延迟队列 - 消费者
 * 监听原Topic，消费到期后的普通消息（无需关注延迟逻辑，与普通消息消费一致）
 */
@Slf4j
public class RocketMQ4xDelayConsumer {
    // 1. 核心配置（需与生产者一致，根据自身环境修改）
    private static final String CONSUMER_GROUP = "delay_consumer_group_4x";
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "delay_topic_4x";
    private static final String TAGS = "delay_tag_4x"; // 与生产者标签一致，过滤消息

    public static void main(String[] args) {
        // 2. 初始化消费者（Push模式，自动拉取消息）
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        // 设置NameServer地址
        consumer.setNamesrvAddr(NAMESRV_ADDR);

        try {
            // 3. 订阅Topic和Tags（*表示订阅所有标签）
            consumer.subscribe(TOPIC, TAGS);

            // 4. 注册消息消费监听器（手动ACK，确保消息消费完成）
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                // 循环处理消息（批量消费，默认批量拉取1条，可配置）
                for (MessageExt msg : msgs) {
                    // 读取消息内容
                    String messageContent = new String(msg.getBody());
                    // 计算实际延迟时间（当前时间 - 消息发送时间）
                    long actualDelay = System.currentTimeMillis() - msg.getBornTimestamp();
                    log.info("消费4.x版本延迟消息：{}，消息ID：{}，实际延迟时间：{}ms",
                            messageContent, msg.getMsgId(), actualDelay);

                    // 模拟业务逻辑处理（如订单超时关闭）
                    // doBusiness(msg);
                }
                // 手动ACK：返回消费成功，RocketMQ删除消息；返回消费失败，会重试
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            // 5. 启动消费者
            consumer.start();
            log.info("RocketMQ 4.x 延迟队列消费者启动成功，等待消费到期消息...");

            // 让消费者一直运行（避免主线程退出）
            System.in.read();
        } catch (MQClientException e) {
            log.error("RocketMQ 4.x 消费者启动失败，原因：{}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("RocketMQ 4.x 消息消费失败，原因：{}", e.getMessage(), e);
        } finally {
            // 可选：关闭消费者（实际生产环境一般不主动关闭）
            // if (consumer != null) {
            //     consumer.shutdown();
            //     log.info("RocketMQ 4.x 消费者关闭成功！");
            // }
        }
    }

    /**
     * 模拟业务逻辑处理（示例：订单超时关闭）
     */
    private static void doBusiness(MessageExt msg) {
        String messageContent = new String(msg.getBody());
        if (messageContent.contains("订单")) {
            log.info("执行订单超时关闭逻辑，消息内容：{}", messageContent);
        }
    }
}