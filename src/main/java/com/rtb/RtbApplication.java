package com.rtb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * RTB 实时竞价广告投放引擎 · 启动类
 *
 * <p>中间件（MySQL / Redis / Kafka）由docker compose 手动启动，
 * 本服务只负责连接，不做容器编排。
 */
@SpringBootApplication
public class RtbApplication {

    public static void main(String[] args) {
        SpringApplication.run(RtbApplication.class, args);
    }
}
