package org.milk.boiling.notification.emitter.config;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.MicrometerProducerListener;
import reactor.kafka.sender.SenderOptions;

@Configuration
@Slf4j
public class KafkaProducerConfig {

    @Bean
    public ReactiveKafkaProducerTemplate<String, Object> producerFactory(KafkaProperties kafkaProperties, MeterRegistry meterRegistry) {
        SenderOptions<String, Object> stringSenderOptions = SenderOptions.create(kafkaProperties.buildProducerProperties(null));
        stringSenderOptions.producerListener(new MicrometerProducerListener(meterRegistry));
        return new ReactiveKafkaProducerTemplate<>(stringSenderOptions);
    }

}
