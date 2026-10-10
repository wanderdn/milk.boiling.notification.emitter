package org.milk.boiling.notification.emitter.service.websocket;

import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.milk.boiling.notification.dto.websocket.UserWebSocketCommand;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
//@ConditionalOnProperty(value = "app.sub.transport", havingValue = "websocket")
public class WebSocketCommandDispatchService {

    private final Map<MilkCommandType, CommandDispatcher<DTOInterface>> dispatchers;

    public WebSocketCommandDispatchService(List<CommandDispatcher<DTOInterface>> commandDispatchersList) {
        this.dispatchers = commandDispatchersList.stream().collect(Collectors.toMap(CommandDispatcher::getType, dispatcher -> dispatcher));
        log.error(dispatchers.toString());
    }

    public Mono<Void> dispatch(UserWebSocketCommand userWebSocketCommand) {
        return dispatchers.getOrDefault(userWebSocketCommand.type(), new CommandDispatcher<DTOInterface>() {
            @Override
            public MilkCommandType getType() {
                return null;
            }

            @Override
            public Mono<Void> process(DTOInterface object) {
                return Mono.error(new RuntimeException("dispatcher not found for command type " + userWebSocketCommand.type()));
            }
        }).process(userWebSocketCommand.payload());
    }


}
