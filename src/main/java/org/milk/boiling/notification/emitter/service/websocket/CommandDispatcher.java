package org.milk.boiling.notification.emitter.service.websocket;

import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;
import reactor.core.publisher.Mono;

public interface CommandDispatcher<T extends DTOInterface> {

    public MilkCommandType getType();

    public Mono<Void> process(T object);

}
