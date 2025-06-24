package org.milk.boiling.notification.emitter.dto;

import java.util.UUID;

public record MilkBoilingEventStatus(UUID eventId, MILK_STATUS milkStatus) {
}
