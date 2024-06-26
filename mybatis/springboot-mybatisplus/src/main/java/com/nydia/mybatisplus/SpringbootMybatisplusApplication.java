package com.nydia.mybatisplus;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.nydia.mybatisplus.mapper")
public class SpringbootMybatisplusApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringbootMybatisplusApplication.class, args);
	}

}
