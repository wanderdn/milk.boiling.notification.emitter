package org.milk.boiling.notification.emitter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEventStatus;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaMessagingService implements SmartLifecycle {

    private final ReactiveKafkaProducerTemplate<String, Object> producerTemplate;
    private final Scheduler scheduler = Schedulers.fromExecutor(Executors.newVirtualThreadPerTaskExecutor());
    volatile boolean running = false;

    public void sendStatus(MilkBoilingEventStatus event) {

        producerTemplate.send("milk.boiling.event.status", event)
                .doOnError(t -> log.error("STATUS NOT SEND FOR {} in reason of ", event, t))
                .onErrorResume(_ -> Mono.empty())
                .subscribeOn(scheduler).subscribe();
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        scheduler.dispose();
        log.info("Kafka Messaging Service has been disposed: {}", scheduler.isDisposed());
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
