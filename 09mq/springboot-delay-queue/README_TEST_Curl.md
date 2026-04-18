# Stream延迟队列 - Curl测试命令

## 1. 发送延迟消息

### 1.1 自动生成taskId发送延迟消息

```bash
curl -X POST "http://localhost:8088/api/stream/delay/send?payload=Hello%20Delay&delay=5&unit=SECONDS"
```

### 1.2 使用指定taskId发送延迟消息

```bash
curl -X POST "http://localhost:8088/api/stream/delay/send/my-task-001?payload=Hello%20Delay&delay=10&unit=SECONDS"
```

## 2. 获取配置信息

```bash
curl -X GET "http://localhost:8088/api/stream/delay/config"
```

## 3. 测试示例

### 3.1 发送5秒延迟消息

```bash
curl -X POST "http://localhost:8088/api/stream/delay/send?payload=TestMessage&delay=5&unit=SECONDS"
```

### 3.2 发送1分钟延迟消息

```bash
curl -X POST "http://localhost:8088/api/stream/delay/send?payload=TestMessage&delay=1&unit=MINUTES"
```

### 3.3 发送1小时延迟消息

```bash
curl -X POST "http://localhost:8088/api/stream/delay/send?payload=TestMessage&delay=1&unit=HOURS"
```

### 3.4 使用指定taskId发送延迟消息

```bash
curl -X POST "http://localhost:8088/api/stream/delay/send/custom-task-id?payload=CustomTaskMessage&delay=30&unit=SECONDS"
```