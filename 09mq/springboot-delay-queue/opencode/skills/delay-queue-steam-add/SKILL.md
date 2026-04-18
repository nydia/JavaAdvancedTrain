---
name: delay-queue-stream-add
description: 对使用stream实现的mq的延迟队列进行修改
license: MIT
---

# 延时队列功能实现
- 对使用stream实现的mq的延迟队列进行修改

## 使用场景
- 使用mq实现延迟队列

## 工作流程

### 第一步：切换工作目录
1. Java代码的包目录为com.nydia.delay.stream
2. application-rocketmq.yaml
### 第二步：代码修改
1. application-rocketmq.yaml 里面的根节点 stream.delay的配置，沿用spring.cloud.stream的配置，不要搞两套
2. Java代码里面用到这些配置的地方也做同步修改
3. com.nydia.delay.stream.StreamDelayProperties
4. com.nydia.delay.stream.StreamDelayRocketConsumer
5. com.nydia.delay.stream.StreamDelayRocketProducer
