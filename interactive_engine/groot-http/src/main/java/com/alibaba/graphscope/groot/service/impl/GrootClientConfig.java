package com.alibaba.graphscope.groot.service.impl;

import com.alibaba.graphscope.groot.sdk.GrootClient;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrootClientConfig {
    @Bean
    public GrootClient grootClient() {
        return GrootClient.newBuilder().addHost("localhost", 55556).build();
    }
}
