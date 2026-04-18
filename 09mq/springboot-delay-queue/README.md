# 延迟队列

## 实现方案
### redis  Redisson
添加任务（5 秒后执行）
curl http://localhost:8088/delay/add?taskId=order_1001&delaySeconds=5
取消任务
curl http://localhost:8088/delay/remove?taskId=order_1001
### rabbitmq ttl 死信队列
curl http://localhost:8088/rabbit/send?msg=order_1001&delay=5000
### rocketmq

### stream
同时支持rabbitmq、rocketmq

### delayqueue
同时支持redis、rabbitmq、rocketmq


## yaml文件说明
1. application-dev1.yaml rabbitmq
2. application-dev2.yaml rocketmq
3. application-dev3.yaml rocketmq、rabbitmq
4. application-dev4.yaml rocketmq、rabbitmq、redis



## 延迟队列配置
### rabbitmq
exchange: 
    name: delay-exchange
    type: topic
queue:
    name: delay-queue
    x-queue-type: classic

