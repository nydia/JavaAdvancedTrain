package com.nydia.delay.rabbitmq;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;

@RestController(value = "redisDelayController")
@RequestMapping("/rabbit")
public class DelayController {

    @Resource
    private DelayProducer delayProducer;

    @GetMapping("/send")
    public String send(
            @RequestParam String msg,
            @RequestParam(defaultValue = "5000") long delay
    ) {
        delayProducer.sendDelayMsg(msg, delay);
        return "发送延迟消息成功：" + msg;
    }
}