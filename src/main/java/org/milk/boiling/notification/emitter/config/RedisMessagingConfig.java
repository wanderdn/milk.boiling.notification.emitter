package org.milk.boiling.notification.emitter.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;


@Configuration
class RedisShardingConfig {
    @Bean
    public RedisMessagingConfig redisMessagingConfig(@Value("${milk.boiling.redis.messaging.stream-name}") String streamName, LettuceConnectionFactory lettuceConnectionFactory) {
        int shards = Optional.ofNullable(lettuceConnectionFactory.getClusterConfiguration()).map(RedisClusterConfiguration::getClusterNodes).map(Set::size).orElse(1) * 2;
        return new RedisMessagingConfig(shards, streamName);
    }

}


public record RedisMessagingConfig(int shardsCount, String streamName) {
    static UUID podId = UUID.randomUUID();

    @Override
    public int shardsCount() {
        return shardsCount;
    }


    public UUID getPodId() {

        return podId;

    }

    public String getPodShardName(int shardId) {

        return "{shard-" + shardId + "}";
    }

    public String getStreamNameForShard(String shardName) {
        return String.join(":", streamName, podId.toString(), shardName);
    }

}
