package net.beeapm.demo.controller;

import com.alibaba.fastjson.JSON;
import net.beeapm.demo.model.RequestVo;
import net.beeapm.demo.model.ResultVo;
import net.beeapm.demo.service.IHelloService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author agent
 * @date 2020/01/12
 */
@RequestMapping("/hello")
@RestController
public class HelloController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private IHelloService service;

    @RequestMapping("/sayGoodbye")
    public ResultVo sayGoodbye(@RequestBody RequestVo req){
        logger.debug(JSON.toJSONString(req));
        return service.sayGoodbye(req);
    }

    @RequestMapping("/sayHello")
    public ResultVo sayHello(@RequestBody RequestVo req){
        logger.debug(JSON.toJSONString(req));
        logger.trace("Log4j2==================>trace");
        logger.debug("Log4j2==================>debug");
        logger.info("Log4j2==================>info");
        logger.warn("Log4j2==================>warn");
        logger.error("Log4j2==================>error");

        return service.sayHello(req);
    }

    @RequestMapping("/welcomeChina")
    public ResultVo welcomeChina(@RequestBody RequestVo req){
        logger.debug(JSON.toJSONString(req));
        return service.welcomeChina(req);
    }

    @RequestMapping("/welcomeXiaMen")
    public ResultVo welcomeXiaMen(@RequestBody RequestVo req){
        logger.debug(JSON.toJSONString(req));
        return service.welcomeXiaMen(req);
    }
}
