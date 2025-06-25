package org.milk.boiling.notification.emitter.service;

import java.util.UUID;

public interface RemoteUserSubscriptionService {


    void removeSubscription(UUID userId);

    void createSubscription(UUID userId);

}
