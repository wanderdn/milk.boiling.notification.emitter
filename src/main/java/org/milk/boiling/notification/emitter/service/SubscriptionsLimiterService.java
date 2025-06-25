package org.milk.boiling.notification.emitter.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SubscriptionsLimiterService {

    private final Mono<Boolean> booleanMono = Mono.just(true);
    private final AtomicInteger maxConnections;


    public SubscriptionsLimiterService() {
        maxConnections = new AtomicInteger(100);
        Gauge.builder("allowed.connections", maxConnections::get).register(Metrics.globalRegistry);
    }

    public Mono<Boolean> isSubscriptionAllowed() {
        var connectionPoolSize = maxConnections.getAndDecrement();
        if (connectionPoolSize <= 0) {
            maxConnections.getAndIncrement();
            return Mono.error(new IllegalStateException("ConnectionPool size must be greater than 0"));
        } else {
            return booleanMono;
        }
    }

    public void releaseConnection() {

        maxConnections.getAndIncrement();

    }
}
