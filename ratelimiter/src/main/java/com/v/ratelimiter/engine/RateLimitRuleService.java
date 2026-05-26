package com.v.ratelimiter.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.v.ratelimiter.domain.RateLimitRule;
import com.v.ratelimiter.domain.RuleUpdateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateLimitRuleService implements MessageListener {

    private final Map<String, RateLimitRule> ruleCache = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private static final RateLimitRule DEFAULT_RULE = new RateLimitRule(10, 0.16);

    public RateLimitRule getRule(String clientId) {
        return ruleCache.getOrDefault(clientId, DEFAULT_RULE);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String payload = new String(message.getBody());
            RuleUpdateEvent event = objectMapper.readValue(payload, RuleUpdateEvent.class);

            ruleCache.put(event.clientId(), event.rule());
            log.info("Rule updated in real-time for client [{}]: Capacity {}",
                    event.clientId(), event.rule().capacity());
        } catch (Exception e) {
            log.error("Failed to parse rule update from Redis Pub/Sub", e);
        }
    }
}