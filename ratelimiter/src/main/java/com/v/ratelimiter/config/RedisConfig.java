package com.v.ratelimiter.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.v.ratelimiter.engine.RateLimitRuleService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
public class RedisConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    // When you publish to the "rate-limit-rules" channel (e.g. via redis-cli or
    // an admin endpoint), RateLimitRuleService.onMessage() picks it up and
    // hot-reloads the rule in memory — no restart needed.
    @Bean
    public RedisMessageListenerContainer redisListenerContainer(
            RedisConnectionFactory factory,
            RateLimitRuleService rateLimitRuleService) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.addMessageListener(rateLimitRuleService, new PatternTopic("rate-limit-rules"));
        return container;
    }
}