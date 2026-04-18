package com.nydia.delay.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@RestController
@RequestMapping("/api/stream/delay")
@RequiredArgsConstructor
public class StreamDelayController {

    private final StreamDelayService streamDelayService;

    @PostMapping("/send")
    public Map<String, Object> sendDelayMessage(
            @RequestParam String payload,
            @RequestParam(defaultValue = "5") long delay,
            @RequestParam(defaultValue = "SECONDS") String unitStr) {

        TimeUnit unit;
        try {
            unit = TimeUnit.valueOf(unitStr.toUpperCase());
        } catch (Exception e) {
            unit = TimeUnit.SECONDS;
        }

        String taskId = streamDelayService.sendDelayMessage(payload, delay, unit);

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("taskId", taskId);
        result.put("payload", payload);
        result.put("delay", delay);
        result.put("unit", unitStr);

        return result;
    }

    @PostMapping("/send/{taskId}")
    public Map<String, Object> sendDelayMessageWithId(
            @PathVariable String taskId,
            @RequestParam String payload,
            @RequestParam(defaultValue = "5") long delay,
            @RequestParam(defaultValue = "SECONDS") String unitStr) {

        TimeUnit unit;
        try {
            unit = TimeUnit.valueOf(unitStr.toUpperCase());
        } catch (Exception e) {
            unit = TimeUnit.SECONDS;
        }

        String returnedTaskId = streamDelayService.sendDelayMessage(taskId, payload, delay, unit);

        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("taskId", returnedTaskId);
        result.put("payload", payload);
        result.put("delay", delay);
        result.put("unit", unitStr);

        return result;
    }

    @GetMapping("/config")
    public Map<String, Object> getConfig() {
        Map<String, Object> result = new HashMap<>();
        result.put("binder", "rabbit/rocket (configurable via spring.cloud.stream.default-binder)");
        result.put("rabbitmq", "Supported via spring-cloud-starter-stream-rabbit");
        result.put("rocketmq", "Supported via spring-cloud-starter-stream-rocketmq");
        return result;
    }
}