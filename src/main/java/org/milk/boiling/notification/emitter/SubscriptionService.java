package org.milk.boiling.notification.emitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.dto.MILK_STATUS;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEventStatus;
import org.milk.boiling.notification.emitter.dto.MilkSubDTO;
import org.milk.boiling.notification.emitter.dto.OPERATION_TYPE;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static reactor.core.publisher.SignalType.CANCEL;
import static reactor.core.publisher.SignalType.ON_COMPLETE;
import static reactor.core.publisher.SignalType.ON_ERROR;

@Service
@RequiredArgsConstructor
@Slf4j
public class SubscriptionService {


    private final ObjectMapper objectMapper;

    private final Function<UUID, Integer> shardingCalcFunction;

    private final Map<Integer, Flux<MapRecord<String, Object, Object>>> shardedFluxMap;

    private final KafkaMessagingService kafkaMessagingService;
    Set<SignalType> signalTypes = Set.of(ON_ERROR, ON_COMPLETE, CANCEL);

    public Flux<MilkBoilingEvent> subscribe(UUID userId) {
        UUID sessionId = UUID.randomUUID();
        var shard = shardingCalcFunction.apply(userId);
        var sub = new MilkSubDTO(sessionId, userId, OPERATION_TYPE.SUBSCRIBE, shard);
        return
                Optional.ofNullable(shardedFluxMap.get(shard))
                        .orElseThrow(() -> new IllegalStateException("Shard not found: " + shard))
                        .map(this::getMilkBoilingEvent).filter(x -> Objects.equals(x.userId(), userId))
                        .doFinally(signalType -> sendUnsubscribeOnEnd(userId, signalType, sessionId, shard))
                        .doOnError(throwable -> log.error("", throwable))
                        .onErrorResume(_ -> Mono.empty())
                        .doOnNext(milkBoilingEvent -> kafkaMessagingService.sendStaus(new MilkBoilingEventStatus(milkBoilingEvent.eventId(), MILK_STATUS.SEND))).
                        onErrorComplete().doOnSubscribe(_ -> kafkaMessagingService.sendSub(sub));
    }

    private void sendUnsubscribeOnEnd(UUID userId, SignalType signalType, UUID sessionId, Integer shard) {
        if (signalTypes.contains(signalType)) {
            kafkaMessagingService.sendSub(new MilkSubDTO(sessionId, userId, OPERATION_TYPE.UNSUBSCRIBE, shard));
        }
    }

    private MilkBoilingEvent getMilkBoilingEvent(MapRecord<String, Object, Object> x) {
        return objectMapper.convertValue(x.getValue(), MilkBoilingEvent.class);
    }


}