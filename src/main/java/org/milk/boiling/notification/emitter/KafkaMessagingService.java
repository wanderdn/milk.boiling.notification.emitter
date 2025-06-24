package org.milk.boiling.notification.emitter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEventStatus;
import org.milk.boiling.notification.emitter.dto.MilkSubDTO;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaMessagingService {

    private final ReactiveKafkaProducerTemplate<String, Object> producerTemplate;


    public void sendStaus(MilkBoilingEventStatus event) {

        producerTemplate.send("milk.boiling.event.status", event)
                .doOnError(t -> log.error("STATUS NOT SEND FOR {} in reason of ", event, t))
                .onErrorResume(_ -> Mono.empty())
                .subscribe();
    }

    public void sendSub(MilkSubDTO subDTO) {
        producerTemplate.send("milk.boiling.subscription.topic", subDTO)
                .doOnError(throwable -> log.error("Unexpected error due creating sub for userId {} ", subDTO.userId(), throwable))
                .onErrorResume(_ -> Mono.empty())
                .subscribe();
    }
}
