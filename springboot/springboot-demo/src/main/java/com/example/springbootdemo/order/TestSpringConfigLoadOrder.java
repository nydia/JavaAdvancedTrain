package com.example.springbootdemo.order;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;

import javax.annotation.PostConstruct;


/**
 * spring的扩展点加载顺序：
 * 启动顺序:BeanFactoryAware # setBeanFactory
 * 启动顺序:BeanFactoryAware # setBeanName
 * 启动顺序:BeanFactoryAware # setBeanFactory
 * 启动顺序:ApplicationContextAware # setApplicationContext
 * 启动顺序:post-construct
 * 启动顺序:InitializingBean # afterPropertiesSet
 * LiveReload server is running on port 35729
 * 启动顺序:SmartInitializingSingleton #
 * Tomcat started on port(s): 8085 (http) with context path ''
 * 启动顺序:Lifecycle # isRunning
 * 启动顺序:Lifecycle # start
 * 启动顺序:ApplicationListener # onApplicationEvent
 * Started SpringbootDemoApplication2 in 4.099 seconds (JVM
 * 启动顺序:CommandLineRunner # run
 */
@Slf4j
@Configuration
public class TestSpringConfigLoadOrder implements
        ApplicationContextAware,
        BeanFactoryAware,
        InitializingBean,
        SmartLifecycle,
        BeanNameAware,
        ApplicationListener<ContextRefreshedEvent>,
        CommandLineRunner,
        SmartInitializingSingleton {

    //InitializingBean
    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("启动顺序:InitializingBean # afterPropertiesSet");
    }

    //ApplicationContextAware
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        log.info("启动顺序:ApplicationContextAware # setApplicationContext");
    }

    @PostConstruct
    public void postConstruct() {
        log.info("启动顺序:post-construct");
    }

    //BeanFactoryAware
    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        log.info("启动顺序:BeanFactoryAware # setBeanFactory");
    }

    //BeanFactoryAware
    @Override
    public void setBeanName(String s) {
        log.info("启动顺序:BeanFactoryAware # setBeanName");
    }

    //SmartInitializingSingleton
    @Override
    public void afterSingletonsInstantiated() {
        log.info("启动顺序:SmartInitializingSingleton # afterSingletonsInstantiated");
    }

    //CommandLineRunner
    @Override
    public void run(String... args) throws Exception {
        log.info("启动顺序:CommandLineRunner # run");
    }

    //ApplicationListener
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        log.info("启动顺序:ApplicationListener # onApplicationEvent");
    }

    //Lifecycle
    @Override
    public void start() {
        log.info("启动顺序:Lifecycle # start");
    }

    //Lifecycle
    @Override
    public void stop() {
        log.info("启动顺序:Lifecycle # stop");
    }

    //Lifecycle
    @Override
    public boolean isRunning() {
        log.info("启动顺序:Lifecycle # isRunning");
        return false;
    }
}