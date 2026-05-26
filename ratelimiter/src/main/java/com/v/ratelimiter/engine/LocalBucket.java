package com.v.ratelimiter.engine;

public class LocalBucket {

    private double tokens;
    private double lastRefillSeconds;
    private long lastAccessMs;
    private long lastRedisSyncMs;
    private boolean refreshing;
    private final int capacity;
    private final double refillRate;

    static final long CACHE_TTL_MS = 500;

    public LocalBucket(int capacity, double refillRate) {
        this.capacity          = capacity;
        this.refillRate        = refillRate;
        double now             = System.currentTimeMillis() / 1000.0;
        this.tokens            = capacity;
        this.lastRefillSeconds = now;
        this.lastAccessMs      = System.currentTimeMillis();
        this.lastRedisSyncMs   = 0;
        this.refreshing        = false;
    }

    public synchronized boolean tryConsume() {
        double now = System.currentTimeMillis() / 1000.0;
        double elapsed  = Math.max(0, now - lastRefillSeconds);
        double refilled = Math.min(capacity, tokens + (elapsed * refillRate));
        lastAccessMs = System.currentTimeMillis();
        if (refilled < 1.0) {
            tokens = refilled;
            lastRefillSeconds = now;
            return false;
        }
        tokens = refilled - 1.0;
        lastRefillSeconds = now;
        return true;
    }

    public synchronized boolean acquireRefreshLock() {
        boolean stale = (System.currentTimeMillis() - lastRedisSyncMs) >= CACHE_TTL_MS;
        if (stale && !refreshing) {
            refreshing = true;
            return true;
        }
        return false;
    }

    public synchronized void reconcile(double redisTokens) {
        tokens = redisTokens;
        lastRefillSeconds = System.currentTimeMillis() / 1000.0;
        lastRedisSyncMs = System.currentTimeMillis();
        refreshing = false;
    }

    public synchronized void refreshFailed() {
        refreshing = false;
    }

    public synchronized void postponeRefresh() {
        lastRedisSyncMs = System.currentTimeMillis();
    }

    public synchronized double getTokens()       { return tokens; }
    public synchronized long getLastAccessMs()   { return lastAccessMs; }
    public synchronized long getLastRedisSyncMs(){ return lastRedisSyncMs; }
    public int getCapacity()      { return capacity; }
    public double getRefillRate() { return refillRate; }
}
