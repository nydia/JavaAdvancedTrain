---
name: delay-queue-stream
description: 写出使用stream实现mq的延迟队列
license: MIT
---

# 延时队列功能实现
- 延迟队列使用mq实现，同时支持rabbitmq和rocketmq
- rabbitmq的版本为amqp-client:5.17.1
- rocketmq的版本为rocketmq-client:4.9.4
- 使用springboot的stream功能兼容两种mq的实现
- 使用Java代码实现

## 使用场景
- 需要使用mq实现延迟队列

## 工作流程

### 第一步：切换工作目录
1. Java代码的包目录为com.nydia.delay.stream
2. 配置文件
- application.yaml
- application-rabbit.yaml
- application-rocket.yaml
3.  application-rocket.yaml和application-rabbit.yaml里面的根节点 stream.delay的配置，沿用spring.cloud.stream的配置，不要搞两套

