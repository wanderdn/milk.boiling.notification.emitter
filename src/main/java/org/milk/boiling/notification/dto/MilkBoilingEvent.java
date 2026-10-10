package org.milk.boiling.notification.dto;

import java.util.UUID;

public record MilkBoilingEvent(UUID eventId, UUID userId, String payload) implements DTOInterface {
}
