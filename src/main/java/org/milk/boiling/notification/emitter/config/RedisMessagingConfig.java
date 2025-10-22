package org.milk.boiling.notification.emitter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.UUID;




@ConfigurationProperties(value = "milk.boiling.redis.messaging")
public record RedisMessagingConfig( String streamName,Integer messageCount,UUID podId) {
    public String getStreamNameForPod() {
        return String.join(":", streamName, podId.toString());
    }

}
