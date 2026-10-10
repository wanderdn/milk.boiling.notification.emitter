package org.milk.boiling.notification.emitter.service.websocket;

import lombok.extern.slf4j.Slf4j;
import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class SendMilkBoilingEventService implements CommandDispatcher<DTOInterface> {
    @Override
    public MilkCommandType getType() {
        return MilkCommandType.PAYLOAD;
    }

    @Override
    public Mono<Void> process(DTOInterface object) {
        log.error(object.getPayload().toString());
        return Mono.empty();
    }

}
