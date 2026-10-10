package org.milk.boiling.notification.emitter.service.websocket;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Sinks;

@Configuration
//@ConditionalOnProperty(value = "app.sub.transport", havingValue = "websocket")

public class WSConf {
    @Bean
    public Sinks.Many<Object> sendMessageSink() {
        return Sinks.many().unicast().onBackpressureBuffer();
    }
}
