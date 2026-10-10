package org.milk.boiling.notification.emitter;

import org.milk.boiling.notification.emitter.config.redis.RedisMessagingConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({RedisMessagingConfig.class})
@ComponentScan(basePackages = {"org.milk.boiling.notification"})
public class Application {

    public static void main(String[] args) {

        SpringApplication.run(Application.class, args);
    }

}
