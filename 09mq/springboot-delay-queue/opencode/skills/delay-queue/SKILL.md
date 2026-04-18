---
name: delay-queue
description: 实现延迟队列功能
license: MIT
---

# 延时队列功能实现
- 使用技术： rabbitmq和rocketmq,以及redission
- rabbitmq的版本为: amqp-client:5.17.1
- rocketmq的版本为: rocketmq-client:4.9.4
- redisson的版本为: redisson:3.25.0
- 使用springboot策略模式实现上述功能，同时支持 redis、rabbitmq、rocketmq三种途径的延迟队列

## 使用场景
- 需要实现延迟队列

## 工作流程

### 第一步：切换工作目录
1. Java代码的包目录为com.nydia.delay.delayqueue
2. 配置文件application.yaml

