package org.milk.boiling.notification.emitter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.cache.SubscriptionControlService;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEventStatus;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageRouterService implements SmartLifecycle {


    private final SubscriptionControlService subscriptionControlService;
    private final Flux<MilkBoilingEvent> fluxMilkBoilingEvents;
    private final KafkaMessagingService kafkaMessagingService;
    volatile boolean running = false;
    private Disposable disposableFlux;

    private void routeMessagesToEmitters(MilkBoilingEvent milkBoilingEvent) {
        Optional.ofNullable(subscriptionControlService.getSinkForUserSessionSubscription(milkBoilingEvent.userId()))
                .ifPresent(userSubscriptions -> {
                            var needSendStatus = new AtomicBoolean(true);
                            userSubscriptions.getActiveSessions()
                                    .forEach((key, value) -> {
                                        var sink = value.tryEmitNext(milkBoilingEvent);
                                        if (sink.isFailure()) {
                                            subscriptionControlService.removeUserSessionSubscription(userSubscriptions.getUserId(), key);
                                        } else if (needSendStatus.get()) {
                                            kafkaMessagingService.sendStatus(MilkBoilingEventStatus.buildSentStatus(milkBoilingEvent.eventId()));
                                            needSendStatus.set(false);
                                        }
                                    });
                        }
                );
    }

    @Override
    public void start() {
        disposableFlux = fluxMilkBoilingEvents.subscribe(this::routeMessagesToEmitters);
        running = true;
    }

    @Override
    public void stop() {
        disposableFlux.dispose();
        log.info("MessageRouter Service disposed {}", disposableFlux.isDisposed());
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }


}
