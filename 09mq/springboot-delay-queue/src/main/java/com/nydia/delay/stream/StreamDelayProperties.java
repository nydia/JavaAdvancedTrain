package com.nydia.delay.stream;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "spring.cloud.stream", ignoreUnknownFields = true)
public class StreamDelayProperties {

    private String defaultBinder = "rabbit";

    private String inputChannel = "delay-input";

    private String outputChannel = "delay-output";

    private String group = "delay-group";

    private String exchange = "delay-exchange";

    private String queue = "delay-queue";

    private String routingKey = "delay.routing.key";

    private String dlxExchange = "delay-dlx-exchange";

    private String dlqQueue = "delay-dlq-queue";

    private String dlqRoutingKey = "delay.dlq.routing.key";

    private Integer defaultDelayLevel = 1;

    private boolean enableDlq = true;

    private String topic = "delay-topic";

    private String tag = "delay";

    private String nameServer = "127.0.0.1:9876";

    private RocketMqBinderProperties rocketmq = new RocketMqBinderProperties();

    @Data
    public static class RocketMqBinderProperties {
        private String nameServer = "127.0.0.1:9876";
    }
}
