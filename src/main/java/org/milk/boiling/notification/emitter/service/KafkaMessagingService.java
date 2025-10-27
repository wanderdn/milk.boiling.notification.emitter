package org.milk.boiling.notification.emitter.service;

import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEventStatus;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;

@Service
@Slf4j
public class KafkaMessagingService implements SmartLifecycle {

    private final ReactiveKafkaProducerTemplate<String, Object> producerTemplate;
    private final Scheduler scheduler = Schedulers.fromExecutor(Executors.newWorkStealingPool(2));
    volatile boolean running = false;
    private final Flux<MilkBoilingEventStatus> milkBoilingEventFlux;

    public KafkaMessagingService(ReactiveKafkaProducerTemplate<String, Object> producerTemplate,  Sinks.Many<MilkBoilingEventStatus>  kafkaEmitter) {
        this.producerTemplate = producerTemplate;
        this.milkBoilingEventFlux = kafkaEmitter.asFlux();
    }

    @Override
    public void start() {
        running = true;
        milkBoilingEventFlux.publishOn(scheduler).flatMap( x->
                producerTemplate.send("milk.boiling.event.status", x)
                .doOnError(t -> log.error("STATUS NOT SEND FOR {} in reason of ", x, t))
                .onErrorResume(_ -> Mono.empty())).subscribe();
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
