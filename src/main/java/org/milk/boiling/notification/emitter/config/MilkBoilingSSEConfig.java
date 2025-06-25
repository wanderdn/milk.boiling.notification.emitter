package org.milk.boiling.notification.emitter.config;

import org.milk.boiling.notification.emitter.service.SubscriptionService;
import org.milk.boiling.notification.emitter.service.SubscriptionsLimiterService;
import org.springframework.context.annotation.Bean;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import java.util.UUID;

import static org.springframework.http.HttpStatus.IM_USED;

@Component
public class MilkBoilingSSEConfig {


    @Bean
    public RouterFunction<ServerResponse> controller(SubscriptionService subscriptionService, SubscriptionsLimiterService subscriptionsLimiterService) {
        return RouterFunctions.route().GET("/milkBoilingSse", _ -> subscriptionsLimiterService.isSubscriptionAllowed().flatMap(_ -> ServerResponse.ok().body(subscriptionService.subscribe(

                                UUID.fromString("0198371a-2252-73a6-85a4-d2efb5e1bb69")

                                //                request.headers().asHttpHeaders().get(HttpHeaders.AUTHORIZATION).stream().findFirst().get()
                        ).map(x -> ServerSentEvent.builder().data(x).build()), ServerSentEvent.class))
                        .onErrorResume(IllegalStateException.class, _ -> ServerResponse.status(IM_USED).build()))
                .build();

    }
}
