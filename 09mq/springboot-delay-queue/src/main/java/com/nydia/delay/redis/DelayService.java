package com.nydia.delay.redis;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.RedissonShutdownException;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * @author lvhq
 * @date 2026.03.28
 */
@Slf4j
@Service
public class DelayService {

    @Resource
    private RedissonClient redissonClient;

    private static final String DELAY_QUEUE_NAME = "delay:queue:common";
    private RBlockingQueue<String> blockingQueue;
    private RDelayedQueue<String> delayedQueue;

    private Thread listenerThread;
    private volatile boolean isRunning = true; // 优雅关闭开关

    @PostConstruct
    public void init() {
        blockingQueue = redissonClient.getBlockingQueue(DELAY_QUEUE_NAME);
        delayedQueue = redissonClient.getDelayedQueue(blockingQueue);
        log.info("✅ 延迟队列初始化完成");
        startListen();
    }

    // ====================== 【核心：优雅关闭】 ======================
    @PreDestroy
    public void destroy() {
        log.info("正在关闭延迟队列监听...");

        // 1. 先停止循环
        isRunning = false;

        // 2. 中断阻塞的 take() → 让它立刻醒过来
        if (listenerThread != null) {
            listenerThread.interrupt();
        }

        // 3. 销毁延迟队列（必须）
        try {
            if (delayedQueue != null) {
                delayedQueue.destroy();
            }
        } catch (Exception e) {
            log.error("销毁延迟队列异常", e);
        }

        log.info("✅ 延迟队列已关闭完成");
    }

    // ====================== 监听线程（take() 阻塞） ======================
    private void startListen() {
        listenerThread = new Thread(() -> {
            log.info("✅ 延迟队列监听已启动，等待任务...");

            // 关键：只在运行状态时循环
            while (isRunning) {
                try {
                    // ✅ 这里用 take() 阻塞等，完全符合你的要求
                    String taskId = blockingQueue.take();

                    log.info("🚀 收到延迟任务：{}", taskId);
                    handleTask(taskId);

                } catch (InterruptedException e) {
                    // 被中断 → 说明是服务关闭
                    log.info("监听线程被中断，准备关闭");
                    break; // 直接退出，不循环

                } catch (RedissonShutdownException e) {
                    log.info("Redisson 已关闭，退出监听");
                    break;

                } catch (Exception e) {
                    log.error("处理任务异常", e);
                }
            }

            log.info("✅ 延迟队列监听线程已完全停止");
        }, "delay-queue-listener");

        listenerThread.start();
    }

    // ====================== 业务处理 ======================
    private void handleTask(String taskId) {
        // 你的业务逻辑
        log.info("... 业务逻辑处理...");
    }

    // ====================== 对外方法 ======================
    public void addDelayTask(String taskId, long delay, TimeUnit unit) {
        delayedQueue.offer(taskId, delay, unit);
    }

    public void removeTask(String taskId) {
        delayedQueue.remove(taskId);
    }

}
