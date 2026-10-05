package com.rtb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * RTB 实时竞价广告投放引擎 · 启动类
 *
 * <p>中间件（MySQL / Redis / Kafka）由docker compose 手动启动，
 * 本服务只负责连接，不做容器编排。
 *
 * <p><b>{@code @EnableScheduling} 是 M6 加的</b>，用于预聚合定时刷盘。
 * 刻意不引入 Quartz 之类的调度框架——
 * 本项目只有两个固定周期的任务，Spring 原生的就够，
 * 多一个依赖就多一个面试时要解释的东西。
 */
@SpringBootApplication
@EnableScheduling
public class RtbApplication {

    public static void main(String[] args) {
        SpringApplication.run(RtbApplication.class, args);
    }
}
