package com.v.ratelimiter.domain;

public record RuleUpdateEvent(
        String clientId,
        RateLimitRule rule
) {}