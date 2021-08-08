package com.nydia.rabbit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * @Auther: hqlv
 * @Date: 2021/8/8 00:08
 * @Description:
 */
@Component
@RabbitListener(queues = RabbitConfig.QUEUE_B)
@Slf4j
public class MessageReceiverB {

    public void process(String content){
        log.info("接收处理队列B当中的消息： " + content);
    }

}
