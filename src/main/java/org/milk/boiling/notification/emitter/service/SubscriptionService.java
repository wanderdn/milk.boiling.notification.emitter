package org.milk.boiling.notification.emitter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.cache.SubscriptionControlService;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static reactor.core.publisher.SignalType.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class SubscriptionService {
    private final static Set<SignalType> signalTypes = Set.of(ON_ERROR, ON_COMPLETE, CANCEL);
    private final SubscriptionControlService subscriptionCacheRepo;

    public Flux<MilkBoilingEvent> subscribe(UUID userId) {
        var subSession = UUID.randomUUID();
        return subscriptionCacheRepo.createUserSessionSubscription(userId, subSession)
                .doOnError(throwable -> log.error("Due processing message exception was thrown", throwable))
                .onErrorComplete()
                .doFinally(signalType -> Optional.of(signalType).filter(signalTypes::contains).ifPresent(_ -> subscriptionCacheRepo.removeUserSessionSubscription(userId, subSession)));


    }
}


