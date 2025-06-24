package org.milk.boiling.notification.emitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Configuration
@Slf4j
public class RedisConf {
    @Bean
    public ReactiveRedisTemplate<String, MilkBoilingEvent> milkBoilingEventRedisTemplate(
            ReactiveRedisConnectionFactory factory,
            ObjectMapper objectMapper
    ) {
        Jackson2JsonRedisSerializer<MilkBoilingEvent> serializer =
                new Jackson2JsonRedisSerializer<>(objectMapper, MilkBoilingEvent.class);
        RedisSerializationContext<String, MilkBoilingEvent> context = RedisSerializationContext
                .<String, MilkBoilingEvent>newSerializationContext(new StringRedisSerializer())
                .value(serializer)
                .hashKey(new StringRedisSerializer())
                .hashValue(serializer)
                .build();
        return new ReactiveRedisTemplate<>(factory, context);
    }


    @Bean
    public Map<Integer, Flux<MapRecord<String, Object, Object>>> shardedFluxMap(ReactiveRedisTemplate<String, MilkBoilingEvent> redisTemplate, RedisMessagingConfig shardsCountConfig) {
        var stringObjectObjectReactiveStreamOperations = redisTemplate.opsForStream();
        var shardedFluxMap = new HashMap<Integer, Flux<MapRecord<String, Object, Object>>>();
        Stream.iterate(0, i -> i + 1).limit(shardsCountConfig.shardsCount()).forEach(i -> {
            String podId = shardsCountConfig.getPodId().toString();
            stringObjectObjectReactiveStreamOperations.createGroup(shardsCountConfig.getStreamNameForUser(i), podId).subscribe();
            var flux =
                    stringObjectObjectReactiveStreamOperations.read(Consumer.from(podId, shardsCountConfig.getStreamNameForUser(i)), StreamReadOptions.empty().block(Duration.ofSeconds(5)), StreamOffset.create(shardsCountConfig.getStreamNameForUser(i), ReadOffset.lastConsumed())).publish();
            shardedFluxMap.put(i, flux);
            log.error("shardedFluxMap {}", i);
            flux.subscribe();
        });

        return shardedFluxMap;
    }

}

