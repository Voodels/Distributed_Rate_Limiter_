package com.v.ratelimiter.engine;

import com.v.ratelimiter.domain.RateLimitResult;
import com.v.ratelimiter.domain.RateLimitRule;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RateLimitEngine {

    private final StringRedisTemplate redisTemplate;
    private final RateLimitRuleService ruleService;
    private final LocalBucketStore bucketStore;
    private final MeterRegistry meterRegistry;

    private static final RedisScript<List> TOKEN_BUCKET = RedisScript.of("""
        local key      = KEYS[1]
        local cap      = tonumber(ARGV[1])
        local rate     = tonumber(ARGV[2])
        local now      = tonumber(ARGV[3])
        local ttl      = tonumber(ARGV[4])
        local b        = redis.call('HMGET', key, 'tokens', 'last_refill')
        local t        = tonumber(b[1]) or cap
        local lr       = tonumber(b[2]) or now
        local elapsed  = math.max(0, now - lr)
        local new_t    = math.min(cap, t + elapsed * rate)
        local allowed  = 0
        if new_t >= 1 then
            new_t   = new_t - 1
            allowed = 1
        end
        redis.call('HSET', key, 'tokens', new_t, 'last_refill', now)
        redis.call('EXPIRE', key, ttl)
        return {allowed, math.floor(new_t)}
    """, List.class);

    /**
     * HOT PATH — called on every request.
     *
     * 1. Try local bucket (in-memory, ~0ms)
     * 2. If cache stale → refresh from Redis (authoritative, distributed)
     * 3. Circuit breaker handles Redis failures gracefully
     */
    public RateLimitResult checkAndConsume(String clientId) {
        RateLimitRule rule   = ruleService.getRule(clientId);
        LocalBucket   bucket = bucketStore.getOrCreate(clientId, rule);

        if (bucket.acquireRefreshLock()) {
            try {
                refreshFromRedis(clientId, bucket, rule);
            } catch (Exception e) {
                bucket.refreshFailed();
                log.warn("Redis refresh failed for clientId={}, using stale cache", clientId);
            }
        }

        boolean allowed = bucket.tryConsume();

        if (allowed) {
            meterRegistry.counter("rl.requests.allowed", "clientId", clientId).increment();
        } else {
            meterRegistry.counter("rl.requests.rejected", "clientId", clientId).increment();
        }

        double tokens = bucket.getTokens();
        return new RateLimitResult(allowed, (int) tokens, resetEpoch(tokens, rule));
    }

    /**
     * Refreshes local bucket from Redis — the authoritative source.
     * Wrapped by circuit breaker: after repeated failures, falls through
     * to redisRefreshFallback (no-op) leaving local cache in degraded mode.
     */
    @CircuitBreaker(name = "redisRateLimiter", fallbackMethod = "redisRefreshFallback")
    public void refreshFromRedis(String clientId, LocalBucket bucket, RateLimitRule rule) {
        Timer timer = meterRegistry.timer("rl.redis.latency", "clientId", clientId);

        double now = System.currentTimeMillis() / 1000.0;

        List<?> res = timer.recordCallable(() ->
            redisTemplate.execute(
                TOKEN_BUCKET,
                List.of("rl:" + clientId),
                String.valueOf(rule.capacity()),
                String.valueOf(rule.refillRatePerSecond()),
                String.valueOf(now),
                "3600"
            )
        );

        double redisTokens = ((Number) res.get(1)).doubleValue();
        bucket.reconcile(redisTokens);
    }

    /**
     * Circuit breaker fallback — Redis is down.
     * No-op: local bucket keeps running with stale state.
     * Next refresh attempt happens when CB allows it (5s).
     */
    @SuppressWarnings("unused")
    public void redisRefreshFallback(String clientId, LocalBucket bucket, RateLimitRule rule, Throwable t) {
        bucket.refreshFailed();
        bucket.postponeRefresh();
        meterRegistry.counter("rl.circuitbreaker.fallback", "clientId", clientId).increment();
        meterRegistry.counter("rl.redis.sync.failure").increment();
    }

    /**
     * Epoch seconds when at least 1 token will be available.
     */
    private long resetEpoch(double remaining, RateLimitRule rule) {
        if (remaining >= 1.0) {
            return System.currentTimeMillis() / 1000;
        }
        double seconds = Math.ceil((1.0 - remaining) / rule.refillRatePerSecond());
        return (long)(System.currentTimeMillis() / 1000.0 + seconds);
    }
}
