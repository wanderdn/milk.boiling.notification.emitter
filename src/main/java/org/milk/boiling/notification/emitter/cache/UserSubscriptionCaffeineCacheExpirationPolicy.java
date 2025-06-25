package org.milk.boiling.notification.emitter.cache;

import com.github.benmanes.caffeine.cache.Expiry;
import org.milk.boiling.notification.emitter.entity.UserSubscriptions;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

@Component

public class UserSubscriptionCaffeineCacheExpirationPolicy implements Expiry<UUID, UserSubscriptions> {


    @Override
    public long expireAfterCreate(UUID key, UserSubscriptions value, long currentTime) {
        return value.getActiveSessions().isEmpty() ? Duration.ofSeconds(300).toNanos() : Long.MAX_VALUE;
    }

    @Override
    public long expireAfterUpdate(UUID key, UserSubscriptions value, long currentTime, long currentDuration) {
        return value.getActiveSessions().isEmpty() ? Duration.ofSeconds(300).toNanos() : Long.MAX_VALUE;

    }

    @Override
    public long expireAfterRead(UUID key, UserSubscriptions value, long currentTime, long currentDuration) {
        return value.getActiveSessions().isEmpty() ? Duration.ofSeconds(300).toNanos() : Long.MAX_VALUE;
    }
}

