package org.milk.boiling.notification.emitter.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.milk.boiling.notification.emitter.dto.MilkBoilingEvent;
import reactor.core.publisher.Sinks;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Setter
@Getter
public class UserSubscriptions {

    @JsonIgnore
    private final Map<UUID, Sinks.Many<MilkBoilingEvent>> activeSessions = new ConcurrentHashMap<>();
    @JsonIgnore
    AtomicBoolean wasSubscribed = new AtomicBoolean(false);
    private UUID userId;
    @Setter
    @Getter
    private LocalDateTime lastSeenTime;


    public UserSubscriptions(UUID userId, LocalDateTime lastSeenTime) {
        this.lastSeenTime = lastSeenTime;
        this.userId = userId;
    }

    public static UserSubscriptions createSubscription(UUID userId) {
        return new UserSubscriptions(userId, LocalDateTime.now());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        UserSubscriptions that = (UserSubscriptions) o;
        return Objects.equals(userId, that.userId);
    }


    @Override
    public int hashCode() {
        return Objects.hash(userId);

    }


    @Override
    public String toString() {
        return ", userId=" + userId +
                ", lastSeenTime=" + lastSeenTime +
                '}';
    }
}
