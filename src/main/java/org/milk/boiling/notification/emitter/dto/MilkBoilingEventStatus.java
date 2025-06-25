package org.milk.boiling.notification.emitter.dto;

import java.util.UUID;

public record MilkBoilingEventStatus(UUID eventId, MILK_STATUS milkStatus) {

    public static MilkBoilingEventStatus buildSentStatus(UUID eventId) {
        return new MilkBoilingEventStatus(eventId, MILK_STATUS.SENT);
    }
}
