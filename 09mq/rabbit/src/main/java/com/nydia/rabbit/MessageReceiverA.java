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
@RabbitListener(queues = RabbitConfig.QUEUE_A)
@Slf4j
public class MessageReceiverA {

    public void process(String content){
        log.info("接收处理队列A当中的消息： " + content);
    }

}
