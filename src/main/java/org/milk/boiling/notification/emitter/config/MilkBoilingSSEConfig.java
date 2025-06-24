package org.milk.boiling.notification.emitter.config;

import org.milk.boiling.notification.emitter.SubscriptionService;
import org.springframework.context.annotation.Bean;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.util.UUID;

@Component
public class MilkBoilingSSEConfig {


    @Bean
    public RouterFunction<ServerResponse> controller(SubscriptionService subscriptionService) {
        return RouterFunctions.route().GET("/milkBoilingSse", _ ->
                ServerResponse.ok().body(subscriptionService.subscribe(
                        UUID.randomUUID()
                        //                request.headers().asHttpHeaders().get(HttpHeaders.AUTHORIZATION).stream().findFirst().get()
                ).map(x -> ServerSentEvent.builder().data(x).build()), ServerSentEvent.class)
        ).build();


    }
}
