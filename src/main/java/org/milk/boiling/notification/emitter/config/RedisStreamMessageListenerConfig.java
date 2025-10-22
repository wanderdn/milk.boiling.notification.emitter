package org.milk.boiling.notification.emitter.config;

import io.lettuce.core.RedisBusyException;
import lombok.extern.slf4j.Slf4j;
import org.apache.fury.BaseFury;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.Disposable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
@Slf4j
public class RedisStreamMessageListenerConfig implements SmartLifecycle {
    private final RedisMessagingConfig redisMessagingConfig;
    private final ReactiveStreamOperations<String, Object, byte[]> reactiveStreamOperations;
    private final ReactiveRedisTemplate<String, byte[]> milkBoilingEventRedisTemplate;
    private final AtomicLong processedMessagesCount = new AtomicLong();
    private final AtomicLong processedMessagesCounted = new AtomicLong();
    private final Function<Map<Object, byte[]>, MilkBoilingEvent> mapMilkBoilingEventFunction;
    private final Consumer<Subscription> subscriptionConsumer;
    private final Consumer<MapRecord<String, Object, byte[]>> processedMessageCounterFunction;


    public RedisStreamMessageListenerConfig(RedisMessagingConfig redisMessagingConfig, ReactiveStreamOperations<String, Object, byte[]> reactiveStreamOperations, ReactiveRedisTemplate<String, byte[]> milkBoilingEventRedisTemplate, BaseFury fury) {
        this.redisMessagingConfig = redisMessagingConfig;
        this.reactiveStreamOperations = reactiveStreamOperations;
        this.milkBoilingEventRedisTemplate = milkBoilingEventRedisTemplate;
        this.mapMilkBoilingEventFunction = objectObjectMap -> (MilkBoilingEvent) fury.deserialize(objectObjectMap.get("payload"));


        this.subscriptionConsumer = _ -> log.trace("created redis stream for shard {}", redisMessagingConfig.getStreamNameForPod());
        this.processedMessageCounterFunction = __ -> {processedMessagesCount.incrementAndGet(); processedMessagesCounted.incrementAndGet();} ;
        redisSystemExceptionMonoFunction = ex -> ex.getRootCause() instanceof RedisBusyException ? Mono.empty() : Mono.error(ex);

        unexpectedErrorDueProcessingMessage = throwable -> {
            log.error("Unexpected error due processing message ", throwable);
            return Mono.empty();
        };

    }

    private final Function<RedisSystemException, Mono<? extends String>> redisSystemExceptionMonoFunction;
    private final Function<Throwable, Publisher<? extends MilkBoilingEvent>> unexpectedErrorDueProcessingMessage;


    @Bean
    public Flux<MilkBoilingEvent> shardedFluxMap() {
        var streamName = redisMessagingConfig.getStreamNameForPod();
        var consumer = org.springframework.data.redis.connection.stream.Consumer.from(redisMessagingConfig.podId().toString(), streamName);
        var streamReadOptions = StreamReadOptions.empty().block(Duration.ofSeconds(5)).count(1000);
        var stringStreamOffset = StreamOffset.create(streamName, ReadOffset.lastConsumed());
        return reactiveStreamOperations.createGroup(streamName, redisMessagingConfig.podId().toString())
                .onErrorResume(RedisSystemException.class, redisSystemExceptionMonoFunction)
                .doOnError(throwable -> log.info("group already exists ", throwable))
                .thenMany(reactiveStreamOperations.read(consumer,
                                streamReadOptions,
                                stringStreamOffset)
                        .doOnNext(processedMessageCounterFunction)
                        .map(Record::getValue).map(mapMilkBoilingEventFunction)
                        .onBackpressureBuffer(1000, BufferOverflowStrategy.DROP_OLDEST)
                        .onErrorResume(unexpectedErrorDueProcessingMessage)
                        .repeat()
                        .doOnSubscribe(subscriptionConsumer)
                        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))));

    }


    @Override
    public void start() {

    }

    @Override
    public void stop() {
        var streamName = redisMessagingConfig.getStreamNameForPod();
        reactiveStreamOperations.deleteConsumer(streamName, org.springframework.data.redis.connection.stream.Consumer.from(redisMessagingConfig.podId().toString(), streamName))
                .then(reactiveStreamOperations.destroyGroup(streamName, redisMessagingConfig.podId().toString()))
                .subscribe().dispose();

    }

    @Scheduled(fixedRate = 10000L)
    public Disposable trim() {
        Long count = processedMessagesCount.get();
        return milkBoilingEventRedisTemplate.getConnectionFactory()
                .getReactiveConnection().streamCommands()
                .xLen(ByteBuffer.wrap(redisMessagingConfig.getStreamNameForPod().getBytes()))
                .flatMap(streamLength -> milkBoilingEventRedisTemplate.getConnectionFactory().getReactiveConnection().streamCommands().xTrim(ByteBuffer.wrap(redisMessagingConfig.getStreamNameForPod().getBytes()), streamLength - count, true))
                .doFinally(__ -> {
                    processedMessagesCount.updateAndGet(processedCounter -> processedCounter - count);
                    log.trace("Stream {} is trimmed now, deleted {} messages ", redisMessagingConfig.getStreamNameForPod(), count);
                })
                .subscribe();

    }

    @Override
    public boolean isRunning() {
        return true;
    }
}



