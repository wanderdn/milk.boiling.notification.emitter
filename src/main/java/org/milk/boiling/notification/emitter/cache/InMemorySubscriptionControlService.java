package org.milk.boiling.notification.emitter.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.milk.boiling.notification.emitter.entity.UserSubscriptions;
import org.milk.boiling.notification.emitter.service.RemoteUserSubscriptionService;
import org.milk.boiling.notification.emitter.service.SubscriptionsLimiterService;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class InMemorySubscriptionControlService implements SubscriptionControlService {

    private final Cache<UUID, UserSubscriptions> currentUsers;
    private final SubscriptionsLimiterService subscriptionsLimiterService;
    private final RemoteUserSubscriptionService userSubscriptionService;

    public InMemorySubscriptionControlService(SubscriptionsLimiterService subscriptionsLimiterService,
                                              Expiry<UUID, UserSubscriptions> userSubscriptionCaffeineCacheExpirationPolicy, RemoteUserSubscriptionService userSubscriptionService) {
        this.currentUsers = Caffeine.newBuilder().expireAfter(userSubscriptionCaffeineCacheExpirationPolicy).build();
        this.subscriptionsLimiterService = subscriptionsLimiterService;
        this.userSubscriptionService = userSubscriptionService;
    }


    public Flux<MilkBoilingEvent> createUserSessionSubscription(@NonNull UUID userId, UUID subSession) { // TODO FULL REFACTOR
        Sinks.Many<MilkBoilingEvent> sinks = Sinks.many().unicast().onBackpressureBuffer();
        currentUsers.asMap()
                .compute(userId, (_, userSubscriptionSessions) -> {
                    if (userSubscriptionSessions == null) {
                        userSubscriptionSessions = UserSubscriptions.createSubscription(userId);
                        log.trace("User subscriptions has been created {}", userSubscriptionSessions);
                        userSubscriptionService.createSubscription(userId);
                    }
                    userSubscriptionSessions.getActiveSessions().put(subSession, sinks);
                    return userSubscriptionSessions;
                });
        return sinks.asFlux();
    }

    public void removeUserSessionSubscription(UUID userId, UUID subSession) {

        currentUsers.asMap().computeIfPresent(userId, (_, userSubscriptions) -> {
                    userSubscriptions.getActiveSessions().remove(subSession);
                    log.trace("User subscriptions has been removed {}  session {}", userSubscriptions, subSession);
                    if (userSubscriptions.getActiveSessions().isEmpty()) {
                        subscriptionsLimiterService.releaseConnection();
                        return null;
                    }
                    return userSubscriptions;
                }
        );
    }


    public @Nullable UserSubscriptions getSinkForUserSessionSubscription(UUID userId) {
        return Optional.ofNullable(currentUsers.getIfPresent(userId)).orElseGet(() -> {
            userSubscriptionService.removeSubscription(userId);
            return null;
        });
    }
}
