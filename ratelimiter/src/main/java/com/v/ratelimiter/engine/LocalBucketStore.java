package com.v.ratelimiter.engine;

import com.v.ratelimiter.domain.RateLimitRule;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * Holds all LocalBuckets in memory.
 * Runs a background eviction task to clean up stale clientIds.
 * Eviction prevents unbounded memory growth.
 */
@Slf4j @Component @RequiredArgsConstructor
public class LocalBucketStore {

    private final MeterRegistry meterRegistry;

    // Core store — one LocalBucket per clientId
    private final ConcurrentHashMap<String, LocalBucket> store = new ConcurrentHashMap<>();

    // Virtual thread executor for eviction
    private ScheduledExecutorService evictionExecutor;

    // Evict buckets not seen for more than 10 minutes
    private static final long EVICT_AFTER_MS = 10 * 60 * 1000L;

    @PostConstruct
    void startEviction() {
        evictionExecutor = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("bucket-evict-", 0).factory()
        );
        // Run eviction every 5 minutes
        evictionExecutor.scheduleAtFixedRate(this::evictStaleBuckets, 5, 5, TimeUnit.MINUTES);

        // Gauge — track how many clients are being tracked
        meterRegistry.gauge("rl.local.bucket.count", store, ConcurrentHashMap::size);
    }

    @PreDestroy
    void stopEviction() {
        if (evictionExecutor != null) {
            evictionExecutor.shutdownNow();
        }
    }

    /**
     * Get or create a LocalBucket for this clientId.
     */
    public LocalBucket getOrCreate(String clientId, RateLimitRule rule) {
        return store.computeIfAbsent(clientId,
            id -> new LocalBucket(rule.capacity(), rule.refillRatePerSecond()));
    }

    /**
     * Get existing bucket — returns null if not found.
     * Used by sync logic.
     */
    public LocalBucket get(String clientId) {
        return store.get(clientId);
    }

    /**
     * Remove buckets that haven't synced in EVICT_AFTER_MS.
     * Prevents memory leak for clients that stop sending requests.
     */
    private void evictStaleBuckets() {
        long now = System.currentTimeMillis();
        int before = store.size();

        store.entrySet().removeIf(entry ->
            (now - entry.getValue().getLastAccessMs()) > EVICT_AFTER_MS
        );

        int evicted = before - store.size();
        if (evicted > 0) {
            log.info("Evicted {} stale local buckets", evicted);
            meterRegistry.counter("rl.local.bucket.evicted").increment(evicted);
        }
    }
}
