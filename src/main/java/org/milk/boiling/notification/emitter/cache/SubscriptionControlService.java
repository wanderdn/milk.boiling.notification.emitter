package org.milk.boiling.notification.emitter.cache;

import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.milk.boiling.notification.emitter.entity.UserSubscriptions;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface SubscriptionControlService {

    Flux<MilkBoilingEvent> createUserSessionSubscription(UUID userId, UUID subSession);

    void removeUserSessionSubscription(UUID subscription, UUID sub);


    UserSubscriptions getSinkForUserSessionSubscription(UUID subscription);
}
