package org.milk.boiling.notification.emitter.service;

import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.config.RedisMessagingConfig;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.stereotype.Service;

import java.util.UUID;


@Service
@Slf4j
public class UserRedisSubscriptionService implements RemoteUserSubscriptionService {

    private final ReactiveSetOperations<String, String> reactiveSetOps;
    private final RedisMessagingConfig messagingConfig;

    public UserRedisSubscriptionService(ReactiveRedisTemplate<String, String> milkBoilingEventRedisTemplate, RedisMessagingConfig messagingConfig) {
        this.reactiveSetOps = milkBoilingEventRedisTemplate.opsForSet();
        this.messagingConfig = messagingConfig;
    }


    public void removeSubscription(UUID userId) {
        reactiveSetOps.remove(userId.toString(), messagingConfig.getPodId().toString()).subscribe();
    }

    public void createSubscription(UUID userId) {
        reactiveSetOps.add(userId.toString(), messagingConfig.getPodId().toString()).subscribe();
    }
}
