package com.v.ratelimiter.domain;

public record RateLimitRule (
    int capacity,
    double refillRatePerSecond
){};
