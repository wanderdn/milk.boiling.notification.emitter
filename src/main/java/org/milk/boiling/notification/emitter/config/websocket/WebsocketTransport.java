package org.milk.boiling.notification.emitter.config.websocket;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.dto.SerializationService;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.milk.boiling.notification.emitter.service.websocket.WebSocketCommandDispatchService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;

@Component
@Slf4j
//@ConditionalOnProperty(value = "app.sub.transport", havingValue = "websocket")
public class WebsocketTransport {
    DataBufferFactory bufferFactory = new DefaultDataBufferFactory();
    private final Mono<Void> websession;

    public WebsocketTransport(WebSocketCommandDispatchService webSocketCommandDispatchService, SerializationService serializationService, @Value("${app.sub.transport.url:http://localhost:8080/router}") String url, Sinks.Many<Object> sendMessageSink) throws URISyntaxException {

        log.info("Websocket Client Started" + url);
        websession = new ReactorNettyWebSocketClient().execute(new URI(url),
                session -> sendMessageSink.asFlux().flatMap(x -> session.send(Mono.just(new WebSocketMessage(WebSocketMessage.Type.BINARY, bufferFactory.wrap(serializationService.serialize(x)))))).then()

                        .and(session.receive()
                                .map(webSocketMessage -> {
                                    byte[] bytes = new byte[webSocketMessage.getPayload().readableByteCount()];
                                    webSocketMessage.getPayload().read(bytes);
                                    return (UserWebSocketCommand) serializationService.deserialize(bytes);
                                })
                                .publishOn(Schedulers.fromExecutorService(Executors.newFixedThreadPool(16)))
                                .doOnNext(webSocketCommandDispatchService::dispatch))

        );
    }

    @PostConstruct
    void ini() {
        websession.subscribe();
    }


}
