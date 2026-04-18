# RocketMQ延迟队列具体实现完整代码（4.x+5.0可直接运行）

## 一、前置准备（必做）

### 1. 环境要求

- JDK：1.8及以上（推荐1.8）

- RocketMQ版本：4.x（如4.9.5）、5.x（如5.1.0）

- Maven：3.6及以上

- RocketMQ服务：已部署（单节点/集群均可，确保NameServer和Broker正常运行）

### 2. Maven依赖配置（核心）

统一依赖配置，4.x和5.x版本通用（5.x版本可兼容4.x API，无需额外修改依赖），pom.xml文件如下：

```XML
<!-- RocketMQ核心依赖 -->
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client</artifactId>
    <version>4.9.5</version> <!-- 4.x版本用此版本；5.x版本替换为5.1.0 -->
</dependency>

<!-- 日志依赖（避免控制台日志报错） -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>1.7.36</version>
</dependency>
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-simple</artifactId>
    <version>1.7.36</version>
    <scope>runtime</scope>
</dependency>

<!--  lombok（可选，简化代码） -->
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <version>1.18.24</version>
    <optional>true</optional>
</dependency>
```

### 3. 核心配置说明

代码中需修改3个关键配置，确保与自身RocketMQ环境一致：

- NameServer地址：如 `127.0.0.1:9876`（集群部署用逗号分隔，如`192.168.1.100:9876,192.168.1.101:9876`）

- 生产者组/消费者组：自定义命名，需保证同一组名唯一（如 `delay_producer_group_4x`）

- Topic名称：自定义命名，需提前在RocketMQ中创建（如 `delay_topic_4x`，可通过RocketMQ控制台或命令创建）

---

## 二、RocketMQ 4.x 版本：固定延迟等级 完整实现代码

核心实现：指定延迟等级（1-18级），实现固定时间延迟，包含生产者、消费者完整代码，附带异常处理和关闭资源逻辑，可直接运行。

### 1. 生产者代码（发送固定延迟消息）

```Java
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
```

### 2. 消费者代码（消费到期延迟消息）

```Java
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
```

### 3. 运行说明

- 先启动消费者，再启动生产者（避免消息发送后，消费者未启动导致消息堆积）；

- 生产者发送消息后，消费者会在10s后（延迟等级3）收到消息，控制台打印实际延迟时间；

- 若需修改延迟时间，修改生产者中 `message.setDelayTimeLevel(3)` 的参数（1-18级），对应Broker配置的延迟时间。

---

## 三、RocketMQ 5.0 版本：精准延迟 完整实现代码

核心实现：设置任意毫秒级延迟（如3.5s、1500ms），支持绝对时间戳投递，代码兼容4.x API，同时提供5.x新API示例，附带异常处理和高可用配置。

### 1. 生产者代码（两种API，任选其一）

#### 方式1：兼容4.x API（推荐，老系统升级无需修改代码）

```Java
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.Date;

/**
 * RocketMQ 5.0 精准延迟队列 - 生产者（兼容4.x API）
 * 支持任意毫秒级延迟，直接设置绝对投递时间戳
 */
@Slf4j
public class RocketMQ5xPreciseDelayProducer {
    // 1. 核心配置（需根据自身环境修改）
    private static final String PRODUCER_GROUP = "precise_delay_producer_group_5x";
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "precise_delay_topic_5x";
    private static final String TAGS = "precise_delay_tag_5x";

    public static void main(String[] args) {
        // 2. 初始化生产者（与4.x版本一致，兼容API）
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        // 可选配置：设置消息持久化（默认开启，确保Broker重启消息不丢失）
        producer.setSendMsgTimeout(3000); // 发送超时时间3s

        try {
            producer.start();
            log.info("RocketMQ 5.0 精准延迟队列生产者启动成功！");

            // 3. 循环发送10条精准延迟消息
            for (int i = 1; i <= 10; i++) {
                String messageContent = "RocketMQ 5.0 精准延迟消息 - " + i + "（自定义延迟3500ms）";
                Message message = new Message(TOPIC, TAGS, messageContent.getBytes());

                // 关键：设置精准延迟时间（当前时间 + 3500ms = 延迟3.5秒）
                long deliveryTimeMs = System.currentTimeMillis() + 3500;
                message.setDeliveryTimeMs(deliveryTimeMs);

                // 发送消息
                SendResult sendResult = producer.send(message);
                log.info("精准延迟消息发送成功，消息ID：{}，预计投递时间：{}，延迟时间：3500ms",
                        sendResult.getMsgId(), new Date(deliveryTimeMs));

                Thread.sleep(1000);
            }
        } catch (MQClientException e) {
            log.error("RocketMQ 5.0 生产者启动失败，原因：{}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("RocketMQ 5.0 消息发送失败，原因：{}", e.getMessage(), e);
        } finally {
            if (producer != null) {
                producer.shutdown();
                log.info("RocketMQ 5.0 精准延迟生产者关闭成功！");
            }
        }
    }
}
```

#### 方式2：5.0新版本API（推荐新业务使用，功能更丰富）

```Java
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * RocketMQ 5.0 精准延迟队列 - 生产者（5.0新版本API）
 * 支持更多新特性：批量发送、事务消息、精准延时优化等
 */
@Slf4j
public class RocketMQ5xNewApiProducer {
    // 核心配置
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "precise_delay_topic_5x";
    private static final String TAGS = "precise_delay_tag_5x";

    public static void main(String[] args) {
        // 1. 获取ClientServiceProvider（5.0新API核心入口）
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        // 2. 配置客户端参数
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(NAMESRV_ADDR)
                .build();

        // 3. 初始化生产者
        try (Producer producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setTopic(TOPIC)
                .build()) {
            log.info("RocketMQ 5.0 新API生产者启动成功！");

            for (int i = 1; i <= 10; i++) {
                // 4. 构建消息（5.0新API消息构建方式）
                String messageContent = "RocketMQ 5.0 新API精准延迟消息 - " + i + "（延迟5000ms）";
                Message message = provider.newMessageBuilder()
                        .setTopic(TOPIC)
                        .setTag(TAGS)
                        .setBody(messageContent.getBytes())
                        // 设置精准延迟时间（当前时间 + 5000ms）
                        .setDeliveryTime(System.currentTimeMillis() + 5000)
                        .build();

                // 5. 发送消息（同步发送）
                SendReceipt sendReceipt = producer.send(message);
                log.info("新API精准延迟消息发送成功，消息ID：{}，预计投递时间：{}，延迟时间：5000ms",
                        sendReceipt.getMessageId(), new Date(System.currentTimeMillis() + 5000));

                TimeUnit.MILLISECONDS.sleep(1000);
            }
        } catch (ClientException e) {
            log.error("RocketMQ 5.0 新API生产者启动/发送失败，原因：{}", e.getMessage(), e);
        } catch (InterruptedException e) {
            log.error("线程中断异常：{}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }
}
```

### 2. 消费者代码（兼容4.x API，与5.0新API通用）

```Java
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * RocketMQ 5.0 精准延迟队列 - 消费者（兼容4.x API，与5.0新API生产者通用）
 */
@Slf4j
public class RocketMQ5xPreciseDelayConsumer {
    // 核心配置（需与生产者一致）
    private static final String CONSUMER_GROUP = "precise_delay_consumer_group_5x";
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "precise_delay_topic_5x";
    private static final String TAGS = "precise_delay_tag_5x";

    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);

        // 可选配置：设置消费线程数（默认20个，根据并发量调整）
        consumer.setConsumeThreadMin(10);
        consumer.setConsumeThreadMax(30);
        // 设置消息重试次数（默认16次，可调整）
        consumer.setMaxReconsumeTimes(5);

        try {
            consumer.subscribe(TOPIC, TAGS);

            // 注册消费监听器
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                for (MessageExt msg : msgs) {
                    String messageContent = new String(msg.getBody());
                    long actualDelay = System.currentTimeMillis() - msg.getBornTimestamp();
                    log.info("消费5.0版本精准延迟消息：{}，消息ID：{}，实际延迟时间：{}ms",
                            messageContent, msg.getMsgId(), actualDelay);

                    // 模拟业务逻辑（如预约提醒、订单补偿）
                    // doBusiness(msg);
                }
                // 手动ACK，消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });

            consumer.start();
            log.info("RocketMQ 5.0 精准延迟消费者启动成功，等待消费到期消息...");

            // 保持消费者运行
            System.in.read();
        } catch (MQClientException e) {
            log.error("RocketMQ 5.0 消费者启动失败，原因：{}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("RocketMQ 5.0 消息消费失败，原因：{}", e.getMessage(), e);
        }
    }

    /**
     * 模拟业务逻辑处理
     */
    private static void doBusiness(MessageExt msg) {
        String messageContent = new String(msg.getBody());
        if (messageContent.contains("预约")) {
            log.info("执行预约提醒逻辑，消息内容：{}", messageContent);
        }
    }
}
```

### 3. 5.0版本新API消费者（可选，贴合5.0特性）

```Java
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;

import java.nio.charset.StandardCharsets;

/**
 * RocketMQ 5.0 精准延迟队列 - 消费者（5.0新版本API）
 */
@Slf4j
public class RocketMQ5xNewApiConsumer {
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "precise_delay_topic_5x";
    private static final String CONSUMER_GROUP = "precise_delay_consumer_group_5x_new";

    public static void main(String[] args) {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(NAMESRV_ADDR)
                .build();

        try (PushConsumer consumer = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration)
                .setConsumerGroup(CONSUMER_GROUP)
                .setTopic(TOPIC)
                .setMessageListener(new MessageListener() {
                    @Override
                    public ConsumeResult consume(org.apache.rocketmq.client.apis.message.Message message) {
                        // 消费消息
                        String messageContent = new String(message.getBody(), StandardCharsets.UTF_8);
                        long actualDelay = System.currentTimeMillis() - message.getBornTimestamp();
                        log.info("新API消费精准延迟消息：{}，消息ID：{}，实际延迟时间：{}ms",
                                messageContent, message.getMessageId(), actualDelay);

                        // 模拟业务处理
                        return ConsumeResult.SUCCESS;
                    }
                })
                .build()) {
            log.info("RocketMQ 5.0 新API消费者启动成功，等待消费到期消息...");
            // 保持消费者运行
            Thread.currentThread().join();
        } catch (ClientException e) {
            log.error("RocketMQ 5.0 新API消费者启动失败，原因：{}", e.getMessage(), e);
        } catch (InterruptedException e) {
            log.error("线程中断异常：{}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }
}
```

### 4. 运行说明

- 5.0版本支持两种API，新业务推荐使用5.0新API，老系统可直接使用4.x兼容API，无需修改代码；

- 修改延迟时间：只需调整生产者中 `System.currentTimeMillis() + 3500` 的参数（3500即3.5秒，可设置任意毫秒值）；

- 最大延迟限制：默认72小时（3天），超过会报错，需提前评估业务需求，商业版可扩展。

---

## 四、关键补充（必看）

### 1. 代码运行必备条件

- RocketMQ服务正常运行：启动NameServer（`nohup sh mqnamesrv `&）和Broker（`nohup sh mqbroker -n 127.0.0.1:9876 `&）；

- Topic提前创建：可通过RocketMQ控制台（http://localhost:8080）或命令（`sh mqadmin updateTopic -n 127.0.0.1:9876 -t delay_topic_4x`）创建；

- 依赖版本匹配：4.x版本生产者/消费者依赖需与Broker版本一致，5.x版本可兼容4.x依赖，但推荐使用对应版本依赖。

### 2. 异常处理说明

- 消息发送失败：代码中已添加重试机制（`setRetryTimesWhenSendFailed`），可根据实际需求调整重试次数；

- 消息消费失败：返回 `ConsumeConcurrentlyStatus.RECONSUME_LATER`，RocketMQ会自动重试，重试次数可通过 `setMaxReconsumeTimes` 设置；

- NameServer连接失败：检查NameServer地址是否正确、服务是否正常运行，集群部署需用逗号分隔多个地址。

### 3. 生产环境优化建议

- 生产者：使用异步发送（`producer.send(message, new SendCallback() {})`），提升并发性能，避免同步发送阻塞；

- 消费者：合理设置消费线程数、批量消费大小，避免消费拥堵；核心业务建议开启消息轨迹，便于排查问题；

- 高可用：Broker部署集群，生产者/消费者开启重试机制，消息开启持久化，避免单节点故障导致消息丢失。
> （注：文档部分内容可能由 AI 生成）