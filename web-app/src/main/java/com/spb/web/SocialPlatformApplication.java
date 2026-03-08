package com.spb.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SocialPlatformApplication {
    public static void main(String[] args) {
        // 纯粹的 Spring Boot 启动，再也没有 Flink 的影子了！
        SpringApplication.run(SocialPlatformApplication.class, args);
    }
}