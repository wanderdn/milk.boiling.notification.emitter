package org.milk.boiling.notification.emitter.service.websocket;

import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;
import reactor.core.publisher.Mono;

public class MilkPingWebsocketService implements CommandDispatcher<DTOInterface> {
    @Override
    public MilkCommandType getType() {
        return null;
    }

    @Override
    public Mono<Void> process(DTOInterface object) {
        return null;
    }
}
