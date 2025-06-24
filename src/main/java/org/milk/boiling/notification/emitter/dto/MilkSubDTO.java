package org.milk.boiling.notification.emitter.dto;

import lombok.Builder;

import java.time.LocalDateTime;
import java.util.UUID;

@Builder
public record MilkSubDTO(UUID sessionId, UUID userId,
                         OPERATION_TYPE operationType,
                         LocalDateTime lastSeenTime, int shardId) {

    public MilkSubDTO(UUID sessionId, UUID userId, OPERATION_TYPE operationType, int shardId) {
        this(sessionId, userId, operationType, LocalDateTime.now(), shardId);
    }


}



