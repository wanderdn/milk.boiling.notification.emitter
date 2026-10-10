package org.milk.boiling.notification.dto.websocket;

import org.milk.boiling.notification.dto.DTOInterface;
import org.milk.boiling.notification.dto.MilkCommandType;

public record UserWebSocketCommand(MilkCommandType type, DTOInterface payload) implements DTOInterface {


    public <T> T getPayload() {
        return (T) payload;
    }

}
