package com.nydia.delay.stream;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class StreamDelayService {

    private final StreamDelayProperties properties;
    private final ObjectProvider<StreamDelayProducer> rabbitProducer;
    private final ObjectProvider<StreamDelayRocketProducer> rocketProducer;

    public StreamDelayService(
            StreamDelayProperties properties,
            ObjectProvider<StreamDelayProducer> rabbitProducer,
            ObjectProvider<StreamDelayRocketProducer> rocketProducer) {
        this.properties = properties;
        this.rabbitProducer = rabbitProducer;
        this.rocketProducer = rocketProducer;
    }

    public String sendDelayMessage(String payload, long delay, TimeUnit unit) {
        String binder = properties.getDefaultBinder();
        
        if ("rocket".equalsIgnoreCase(binder)) {
            return rocketProducer.getObject().sendDelayMessage(payload, delay, unit);
        } else {
            return rabbitProducer.getObject().sendDelayMessage(payload, delay, unit);
        }
    }

    public String sendDelayMessage(String taskId, String payload, long delay, TimeUnit unit) {
        String binder = properties.getDefaultBinder();
        
        if ("rocket".equalsIgnoreCase(binder)) {
            return rocketProducer.getObject().sendDelayMessage(taskId, payload, delay, unit);
        } else {
            return rabbitProducer.getObject().sendDelayMessage(taskId, payload, delay, unit);
        }
    }
}