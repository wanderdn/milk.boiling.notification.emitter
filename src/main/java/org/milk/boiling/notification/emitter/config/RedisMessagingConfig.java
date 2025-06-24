package org.milk.boiling.notification.emitter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.UUID;


@ConfigurationProperties(prefix = "milk.boiling.redis.messaging")

public record RedisMessagingConfig(int shardsCount, String streamName) {

    private static final UUID podId = UUID.randomUUID();

    @Override
    public int shardsCount() {
        return shardsCount;
    }


    public UUID getPodId() {
        return podId;
    }

    public String getStreamNameForUser(int shardId) {

        return String.join(":", streamName, podId.toString(), "{shard-" + shardId + "}");


    }
}
