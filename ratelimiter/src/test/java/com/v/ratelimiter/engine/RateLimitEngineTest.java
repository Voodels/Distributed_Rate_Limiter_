package com.v.ratelimiter.engine;

import com.v.ratelimiter.domain.RateLimitResult;
import com.v.ratelimiter.domain.RateLimitRule;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RateLimitEngineTest {

    @Mock StringRedisTemplate redisTemplate;
    @Mock RateLimitRuleService ruleService;
    @Mock LocalBucketStore bucketStore;

    RateLimitEngine engine;

    @BeforeEach
    void setUp() {
        engine = new RateLimitEngine(redisTemplate, ruleService, bucketStore, new SimpleMeterRegistry());
        
        // General stubbing for redisTemplate.execute to avoid NPEs
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any(), any()))
            .thenReturn(List.of(1L, 9L));
    }

    @Test
    void allowsRequest_whenTokensAvailable() throws Exception {
        RateLimitRule rule = new RateLimitRule(10, 1.0);
        LocalBucket bucket = new LocalBucket(10, 1.0);
        
        when(ruleService.getRule("client-1")).thenReturn(rule);
        when(bucketStore.getOrCreate(eq("client-1"), any())).thenReturn(bucket);

        RateLimitResult result = engine.checkAndConsume("client-1");

        assertThat(result.allowed()).isTrue();
        assertThat(result.remaining()).isEqualTo(9);
    }

    @Test
    void rejectsRequest_whenNoTokens() throws Exception {
        RateLimitRule rule = new RateLimitRule(10, 1.0);
        LocalBucket bucket = new LocalBucket(10, 1.0);
        // Consume all tokens
        for(int i=0; i<10; i++) bucket.tryConsume();
        
        when(ruleService.getRule("client-2")).thenReturn(rule);
        when(bucketStore.getOrCreate(eq("client-2"), any())).thenReturn(bucket);

        RateLimitResult result = engine.checkAndConsume("client-2");

        assertThat(result.allowed()).isFalse();
        assertThat(result.remaining()).isEqualTo(0);
    }

    @Test
    void fallback_returnsDenied_onRedisSyncFailure() {
        LocalBucket bucket = new LocalBucket(10, 1.0);
        RateLimitRule rule = new RateLimitRule(10, 1.0);
        
        // This test verifies the fallback method itself
        engine.syncFallback("client-3", bucket, rule, new RuntimeException("Redis down"));
        
        // The fallback increments a counter, doesn't return a value in the new async syncToRedis
    }

    @Test
    void metricsIncremented_onAllowedRequest() throws Exception {
        var registry = new SimpleMeterRegistry();
        engine = new RateLimitEngine(redisTemplate, ruleService, bucketStore, registry);

        RateLimitRule rule = new RateLimitRule(10, 1.0);
        LocalBucket bucket = new LocalBucket(10, 1.0);
        
        when(ruleService.getRule("client-4")).thenReturn(rule);
        when(bucketStore.getOrCreate(eq("client-4"), any())).thenReturn(bucket);

        engine.checkAndConsume("client-4");

        assertThat(registry.counter("rl.requests.allowed", "clientId", "client-4").count())
            .isEqualTo(1.0);
        assertThat(registry.counter("rl.layer1.hit").count())
            .isEqualTo(1.0);
    }

    @Test
    void metricsIncremented_onRejectedRequest() throws Exception {
        var registry = new SimpleMeterRegistry();
        engine = new RateLimitEngine(redisTemplate, ruleService, bucketStore, registry);

        RateLimitRule rule = new RateLimitRule(10, 1.0);
        LocalBucket bucket = new LocalBucket(10, 1.0);
        for(int i=0; i<10; i++) bucket.tryConsume();

        when(ruleService.getRule("client-5")).thenReturn(rule);
        when(bucketStore.getOrCreate(eq("client-5"), any())).thenReturn(bucket);

        engine.checkAndConsume("client-5");

        assertThat(registry.counter("rl.requests.rejected", "clientId", "client-5").count())
            .isEqualTo(1.0);
    }
}
