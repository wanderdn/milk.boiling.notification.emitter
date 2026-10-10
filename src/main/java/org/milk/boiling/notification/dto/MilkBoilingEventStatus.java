package org.milk.boiling.notification.dto;

import java.util.UUID;

public record MilkBoilingEventStatus(UUID eventId, MILK_STATUS milkStatus) implements DTOInterface {

    public static MilkBoilingEventStatus buildSentStatus(UUID eventId) {
        return new MilkBoilingEventStatus(eventId, MILK_STATUS.SENT);
    }
}
