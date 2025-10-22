package org.milk.boiling.notification.emitter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.config.RedisMessagingConfig;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@Slf4j
@RequiredArgsConstructor
public class PodRedisHeartBeatService implements SmartLifecycle {
    private final ReactiveRedisTemplate<String, String> milkBoilingEventRedisTemplate;
    private final RedisMessagingConfig redisMessagingConfig;
    volatile boolean running = false;
    private Disposable disposable;

    @Override
    public void start() {
        disposable = milkBoilingEventRedisTemplate.opsForValue().set("pods:"+ redisMessagingConfig.podId().toString(),"", Duration.ofSeconds(60)) // this one need to be refactored
                .repeat().delayElements(Duration.ofSeconds(10)).retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))).subscribe();
        running = true;
    }

    @Override
    public void stop() {
        if (disposable != null && !disposable.isDisposed()) {
            disposable.dispose();
            log.info("Heartbeat Service disposed {}", disposable.isDisposed());
        }
        running = false;

    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
