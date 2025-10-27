package org.milk.boiling.notification.emitter.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
@Slf4j
public class RedisTemplateConfig {
    @Bean
    public ReactiveRedisTemplate<String, byte[]> milkBoilingEventRedisTemplate(
            ReactiveRedisConnectionFactory factory
    ) {

        RedisSerializationContext<String, byte[]> context = RedisSerializationContext
                .<String, byte[]>newSerializationContext(new StringRedisSerializer())
                .value(RedisSerializer.byteArray())
                .hashKey(new StringRedisSerializer())
                .hashValue(RedisSerializer.byteArray())
                .build();
        return new ReactiveRedisTemplate<>(factory, context);
    }

    @Bean
    public ReactiveStreamOperations<String, Object, byte[]> reactiveStreamOperations(ReactiveRedisTemplate<String, byte[]> milkBoilingEventRedisTemplate) {
        return milkBoilingEventRedisTemplate.opsForStream();
    }
    }
