package com.alibaba.cloud.nacos.ribbon;///*
// * Copyright (C) 2018 the original author or authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.alibaba.cloud.nacos.ribbon;
//
//import com.alibaba.cloud.nacos.NacosDiscoveryProperties;
//import com.netflix.client.config.IClientConfig;
//import com.netflix.loadbalancer.ServerList;
//import com.nydia.nacos.ScadNacosServerList;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
///**
// * integrated Ribbon by default
// * @author xiaojing
// */
//@Configuration
//@ConditionalOnRibbonNacos
//public class NacosRibbonClientConfiguration {
//
//	@Bean
//	@ConditionalOnMissingBean
//	public ServerList<?> ribbonServerList(IClientConfig config,
//			NacosDiscoveryProperties nacosDiscoveryProperties) {
//		ScadNacosServerList serverList = new ScadNacosServerList(nacosDiscoveryProperties);
//		serverList.initWithNiwsConfig(config);
//		return serverList;
//	}
//
//	@Bean
//	@ConditionalOnMissingBean
//	public NacosServerIntrospector nacosServerIntrospector() {
//		return new NacosServerIntrospector();
//	}
//}
