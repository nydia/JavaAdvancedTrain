package com.nydia.delay.redis;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

@RestController(value = "rabbitDelayController")
@RequestMapping("/delay")
@Slf4j
public class DelayController {

    // 注入你的延迟队列服务
    @Resource
    private DelayService delayService;

    /**
     * 添加延迟任务
     *
     * @param taskId       任务ID（如订单号）
     * @param delaySeconds 延迟秒数
     * @return
     */
    @GetMapping("/add")
    public String addDelayTask(
            @RequestParam String taskId,
            @RequestParam(defaultValue = "5") Integer delaySeconds
    ) {
        try {
            delayService.addDelayTask(taskId, delaySeconds, TimeUnit.SECONDS);
            return "✅ 添加延迟任务成功：" + taskId + "，延迟" + delaySeconds + "秒";
        } catch (Exception e) {
            log.error("添加任务失败", e);
            return "❌ 添加失败：" + e.getMessage();
        }
    }

    /**
     * 取消延迟任务
     *
     * @param taskId 要取消的任务ID
     * @return
     */
    @GetMapping("/remove")
    public String removeDelayTask(@RequestParam String taskId) {
        try {
            delayService.removeTask(taskId);
            return "✅ 取消延迟任务成功：" + taskId;
        } catch (Exception e) {
            log.error("取消任务失败", e);
            return "❌ 取消失败：" + e.getMessage();
        }
    }
}