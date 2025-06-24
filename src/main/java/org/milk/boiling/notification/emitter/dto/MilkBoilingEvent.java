package org.milk.boiling.notification.emitter.dto;

import java.util.UUID;

public record MilkBoilingEvent(UUID eventId, UUID userId, String payload) {
}
