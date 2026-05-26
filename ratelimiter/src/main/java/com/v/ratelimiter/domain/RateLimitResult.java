package com.v.ratelimiter.domain;

/**
 * The immutable result returned by the RateLimitEngine.
 */
public record RateLimitResult(
        boolean allowed,
        int remaining,
        long resetTimeEpochSeconds
) {}