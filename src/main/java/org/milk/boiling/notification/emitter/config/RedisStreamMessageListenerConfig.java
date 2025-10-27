package org.milk.boiling.notification.emitter.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.fury.BaseFury;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.ReactiveStreamOperations;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
@Slf4j
public class RedisStreamMessageListenerConfig {
    private final RedisMessagingConfig redisMessagingConfig;
    private final ReactiveStreamOperations<String, Object, byte[]> reactiveStreamOperations;
    private final Consumer<Subscription> subscriptionConsumer;
    private final Function<Throwable, Publisher<? extends List<MilkBoilingEvent>>> unexpectedErrorDueProcessingMessage;
    private final Function<List<MapRecord<String, Object, byte[]>>, Publisher<List<MilkBoilingEvent>>> mapRecordPublisherFunction;
    private final BaseFury fury;

    public RedisStreamMessageListenerConfig(RedisMessagingConfig redisMessagingConfig, ReactiveStreamOperations<String, Object, byte[]> reactiveStreamOperations,
                                            BaseFury fury
    ) {
        this.redisMessagingConfig = redisMessagingConfig;
        this.reactiveStreamOperations = reactiveStreamOperations;
        this.fury = fury;
        this.subscriptionConsumer = _ -> log.trace("created redis stream for shard {}", redisMessagingConfig.getStreamNameForPod());
        this.mapRecordPublisherFunction = stringObjectMapRecord -> {
            var listId = stringObjectMapRecord.stream()
                    .map(Record::getId)
                    .toArray(RecordId[]::new);
            return
                    reactiveStreamOperations.acknowledge(redisMessagingConfig.getStreamNameForPod(), redisMessagingConfig.podId().toString(), listId)
                            .then(reactiveStreamOperations.delete(redisMessagingConfig.getStreamNameForPod(), listId))
                            .then(Mono.just(stringObjectMapRecord.stream().map(this::getMilkBoilingEventFunction).toList()));
        };

        this.unexpectedErrorDueProcessingMessage = throwable -> {
            log.error("Unexpected error due processing message ", throwable);
            return Mono.empty();
        };

    }

    private MilkBoilingEvent getMilkBoilingEventFunction(MapRecord<String, Object, byte[]> mapRecord) {
        try {
            return (MilkBoilingEvent) fury.deserialize(mapRecord.getValue().get("payload"));
        } catch (Exception e) {
            log.error(e.getMessage());
            return null;
        }
    }


    @Bean
    public Flux<List<MilkBoilingEvent>> shardedFluxMap() {
        var streamName = redisMessagingConfig.getStreamNameForPod();
        var streamReadOptions = StreamReadOptions.empty().block(Duration.ofMillis(10)).count(10_000);
        var stringStreamOffset = StreamOffset.create(streamName, ReadOffset.from("0-0"));
        return
                reactiveStreamOperations.read(
                                streamReadOptions,
                                stringStreamOffset
                        ).bufferTimeout(10000, Duration.ofMillis(2))
                        .flatMap(mapRecordPublisherFunction)
                        .onBackpressureBuffer(100, BufferOverflowStrategy.DROP_OLDEST)
                        .onErrorResume(unexpectedErrorDueProcessingMessage)
                        .repeat()
                        .doOnSubscribe(subscriptionConsumer)
                        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)));

    }


}



