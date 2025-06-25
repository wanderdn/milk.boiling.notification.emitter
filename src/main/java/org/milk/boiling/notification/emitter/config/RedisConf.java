package org.milk.boiling.notification.emitter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisBusyException;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.function.Function;

@Configuration
@Slf4j
public class RedisConf {


    @Bean
    public ReactiveRedisTemplate<String, String> milkBoilingEventRedisTemplate(
            ReactiveRedisConnectionFactory factory
    ) {

        RedisSerializationContext<String, String> context = RedisSerializationContext
                .<String, String>newSerializationContext(new StringRedisSerializer())
                .value(new StringRedisSerializer())
                .hashKey(new StringRedisSerializer())
                .hashValue(new StringRedisSerializer())
                .build();
        return new ReactiveRedisTemplate<>(factory, context);
    }

    @Bean
    public ReactiveStreamOperations<String, Object, Object> reactiveStreamOperations(ReactiveRedisTemplate<String, String> milkBoilingEventRedisTemplate) {
        return milkBoilingEventRedisTemplate.opsForStream();
    }


    @Bean
    public Flux<MilkBoilingEvent> shardedFluxMap(ReactiveStreamOperations<String, Object, Object> reactiveStreamOperations, RedisMessagingConfig shardsCountConfig, ObjectMapper objectMapper) {
        return Flux.range(0, shardsCountConfig.shardsCount())
                .map(shardNumber -> shardsCountConfig.getStreamNameForShard(shardsCountConfig.getPodShardName(shardNumber)))
                .map(streamName ->
                        getFlux(reactiveStreamOperations, shardsCountConfig.getPodId().toString(), streamName, objectMapper)).flatMap(Function.identity());
    }


    private Flux<MilkBoilingEvent> getFlux(ReactiveStreamOperations<String, Object, Object> reactiveStreamOperations, String podId, String streamName, ObjectMapper objectMapper) {
        var consumer = Consumer.from(podId, streamName);
        var streamOptions = StreamReadOptions.empty().block(Duration.ofSeconds(5)).count(100);
        var streamOffset = StreamOffset.create(streamName, ReadOffset.lastConsumed());
        return reactiveStreamOperations.createGroup(streamName, podId)
                .onErrorResume(RedisSystemException.class, ex ->
                        ex.getRootCause() instanceof RedisBusyException
                                ? Mono.empty()
                                : Mono.error(ex))
                .doOnError(throwable -> log.info("group already exists ", throwable))
                .thenMany(reactiveStreamOperations.read(consumer, streamOptions, streamOffset)
                        .map(Record::getValue).map(x -> objectMapper.convertValue(x, MilkBoilingEvent.class))
                        .onBackpressureBuffer(1000, BufferOverflowStrategy.DROP_OLDEST)// config it
                        .onErrorResume(throwable -> {
                            log.error("Unexpected error due processing message ", throwable);
                            return Mono.empty();
                        })
                        .repeat()
                        .doOnSubscribe(_ -> log.trace("created redis stream for shard {}", streamName))
                        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)))
                        .doFinally(__ -> reactiveStreamOperations.deleteConsumer(streamName, consumer)
                                .then(reactiveStreamOperations.destroyGroup(streamName, podId))
                                .subscribe().dispose()));

    }


}



