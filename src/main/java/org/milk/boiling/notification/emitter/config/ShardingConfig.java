//package org.milk.boiling.notification.emitter.config;
//
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//import java.math.BigInteger;
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.UUID;
//import java.util.function.Function;
//
//@Configuration
//@Slf4j
//public class ShardingConfig {
//    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
//
//    public ShardingConfig() throws NoSuchAlgorithmException {
//    }
//
//    @Bean
//    public Function<UUID, Integer> shardingCalcFunction(RedisMessagingConfig shardsCountConfig) {
//        return uuid -> new BigInteger(1, digest.digest(uuid.toString().getBytes())).mod(BigInteger.valueOf(shardsCountConfig.shardsCount())).intValue();
//    }
//}
