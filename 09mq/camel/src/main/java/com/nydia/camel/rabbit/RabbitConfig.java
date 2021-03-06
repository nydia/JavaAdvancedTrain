package com.nydia.camel.rabbit;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * @Auther: hqlv
 * @Date: 2021/8/7 16:58
 * @Description:
 */
@Configuration
public class RabbitConfig {

    @Value("${spring.rabbitmq.host}")
    private String host;

    @Value("${spring.rabbitmq.port}")
    private int port;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    public static final String EXCHANGE_A = "my-mq-exchange_A";
    public static final String EXCHANGE_B = "my-mq-exchange_B";
    public static final String EXCHANGE_C = "my-mq-exchange_C";


    public static final String QUEUE_A = "QUEUE_A";
    public static final String QUEUE_B = "QUEUE_B";
    public static final String QUEUE_C = "QUEUE_C";

    public static final String ROUTINGKEY_A = "spring-boot-routingKey_A";
    public static final String ROUTINGKEY_B = "spring-boot-routingKey_B";
    public static final String ROUTINGKEY_C = "spring-boot-routingKey_C";

    @Bean
    public ConnectionFactory connectionFactory(){
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost("/");
        connectionFactory.setPublisherConfirms(true);
        return connectionFactory;
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public RabbitTemplate rabbitTemplate(){
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory());
        return rabbitTemplate;
    }

    /**
     * ?????????????????????
     * 1. ?????????????????????
     * 2. ???????????????????????????
     FanoutExchange: ?????????????????????????????????????????????routingkey?????????
     HeadersExchange ?????????????????????key-value??????
     DirectExchange: ??????routingkey?????????????????????
     TopicExchange:??????????????????
     */
    @Bean
    public DirectExchange directExchange(){
        return new DirectExchange(EXCHANGE_A);
    }

    @Bean
    public Queue queueA(){
        return new Queue(QUEUE_A, true);//????????????
    }

    @Bean
    public Queue queueB() {
        return new Queue(QUEUE_B, true); //????????????
    }

    @Bean
    public Queue queueC() {
        return new Queue(QUEUE_C, true); //????????????
    }

    @Bean
    public Binding bindingA(){
        return BindingBuilder.bind(queueA()).to(directExchange()).with(RabbitConfig.ROUTINGKEY_A);
    }

    @Bean
    public Binding bindingB() {
        return BindingBuilder.bind(queueB()).to(directExchange()).with(RabbitConfig.ROUTINGKEY_B);
    }
    @Bean
    public Binding bindingC(){
        return BindingBuilder.bind(queueC()).to(directExchange()).with(RabbitConfig.ROUTINGKEY_C);
    }





}
