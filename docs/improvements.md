# DRL-1 — 10 Production-Grade Improvements

| # | Improvement | Impact | Effort | Risk |
|---|-------------|--------|--------|------|
| 1 | StampedLock → replace `synchronized` | p50 latency cut 40% | Low | Low |
| 2 | Request collapsing via CompletableFuture | -24/25 Redis calls per TTL window | Medium | Low |
| 3 | Lua script with server-side clock | Eliminates clock skew between replicas | Low | Low |
| 4 | Rate limit response headers | Client compliance, standard practice | Low | Low |
| 5 | Subscription tiers from API key prefix | Zero-cost differentiation | Low | Low |
| 6 | Global + per-client dual bucket in one Lua | Abuse protection, no extra round trips | Medium | Medium |
| 7 | Network latency testing with `tc netem` | Validates p99 under real conditions | Low | None |
| 8 | ZGC garbage collector | Sub-ms pause times at 5000 req/s | Low | Low |
| 9 | Kafka audit log for decisions | Billing/abuse analysis with replay | High | Low |
| 10 | `Retry-After` as HTTP header | Well-known bots back off automatically | Low | Low |

---

## 1. Replace `synchronized` with `StampedLock`

### Problem
`LocalBucket` uses `synchronized` on every method — `tryConsume()`, `acquireRefreshLock()`, `reconcile()`, `refreshFailed()`, `postponeRefresh()`, getters. The intrinsic lock serializes **every access** even though:

- `tryConsume()` is pure local math (~100ns) — called on **every request** (5000 req/s)
- `reconcile()` writes from Redis — called ~40 times/s (once per stale cache per client)
- `acquireRefreshLock()` reads `lastRedisSyncMs` + `refreshing` — called on every request

Under contention, concurrent reads block each other for no reason.

### Solution
Split into read-write with `StampedLock`:

```java
private final StampedLock lock = new StampedLock();

public boolean tryConsume() {
    long stamp = lock.readLock();
    try {
        double now = System.currentTimeMillis() / 1000.0;
        double elapsed = Math.max(0, now - lastRefillSeconds);
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
    } finally {
        lock.unlockRead(stamp);
    }
}

public synchronized boolean acquireRefreshLock() {
    // stays synchronized — short, involves stale check + boolean flip
    long stamp = lock.readLock();
    try {
        boolean stale = (System.currentTimeMillis() - lastRedisSyncMs) >= CACHE_TTL_MS;
        if (stale && !refreshing) {
            refreshing = true;
            return true;
        }
        return false;
    } finally {
        lock.unlockRead(stamp);
    }
}

public void reconcile(double redisTokens) {
    long stamp = lock.writeLock();
    try {
        tokens = Math.min(tokens, redisTokens);
        lastRefillSeconds = System.currentTimeMillis() / 1000.0;
        lastRedisSyncMs = System.currentTimeMillis();
        refreshing = false;
    } finally {
        lock.unlockWrite(stamp);
    }
}
```

> **Note**: `acquireRefreshLock()` can stay as `readLock` + volatile boolean, or upgrade to `tryConvertToWriteLock` for the `refreshing` assignment. Simpler: keep `refreshing` as `volatile` and use CAS: `AtomicBoolean.compareAndSet(false, true)` outside the lock entirely.

### Acceptance Criteria
- p50 latency drops from ~1.5µs to sub-µs under 5000 req/s
- No thread contention observed in JFR profiles
- Correctness verified: `tryConsume()` never sees torn writes from `reconcile()`

---

## 2. Request Collapsing (TTL Batching)

### Problem
When 25 VUs hit the same client simultaneously at the 500ms TTL boundary, 24 acquire the lock first. That's good — one winner refreshes from Redis. But the 24 losers immediately return to `tryConsume()` with stale data. The winner's Redis result is wasted on all but one request.

Worse: if refill is slow (0.16 t/s) and cache was stale by 1 token, 24 requests get a stale `false` (rejected) when the fresh Redis state could have given them `true`.

### Solution
When `acquireRefreshLock()` is true, create a `CompletableFuture<Double>` stored on the bucket. All subsequent callers that lose the lock wait on the same future (up to 200ms). The winner completes the future with `reconcile()`.

```java
// In LocalBucket
private CompletableFuture<Double> pendingRefresh;

public CompletableFuture<Double> getPendingRefresh() {
    return pendingRefresh;
}

public void setPendingRefresh(CompletableFuture<Double> f) {
    this.pendingRefresh = f;
}
```

```java
// In RateLimitEngine
public RateLimitResult checkAndConsume(String clientId) {
    RateLimitRule rule = ruleService.getRule(clientId);
    LocalBucket bucket = bucketStore.getOrCreate(clientId, rule);

    if (bucket.acquireRefreshLock()) {
        CompletableFuture<Double> future = new CompletableFuture<>();
        bucket.setPendingRefresh(future);
        try {
            refreshFromRedis(clientId, bucket, rule);
            future.complete(bucket.getTokens());
        } catch (Exception e) {
            bucket.refreshFailed();
            future.completeExceptionally(e);
        }
    } else if (bucket.getPendingRefresh() != null) {
        // Collapse: wait for the in-flight refresh
        try {
            bucket.getPendingRefresh().get(200, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // timeout or failure — use stale cache, it's fine
        }
    }

    boolean allowed = bucket.tryConsume();
    // ...
}
```

### Acceptance Criteria
- Redis call rate drops from ~40/s to effectively ~1 per client per 500ms
- All 25 concurrent requests see the **same** Redis result
- p95 latency unaffected (winner's Redis RTT absorbed by others' wait)

---

## 3. Lua Script with Redis-Side Clock

### Problem
The Lua script receives `now` from the Java side (`System.currentTimeMillis() / 1000.0`). In multi-replica deployments, NTP drift between pods (`±1ms`) causes subtle race conditions: if pod A sets `last_refill = now_A` and pod B reads it with `elapsed = now_B - last_refill`, the elapsed time can be slightly negative (or larger than real).

For `0.16 t/s` this is negligible. For pro/enterprise tiers at `5 t/s` or `50 t/s`, 1ms clock skew = 0.005–0.05 tokens of error per request — accumulates.

### Solution
Remove the `now` parameter from the Lua script. Compute `elapsed` directly inside Redis using `redis.call('TIME')` which returns the Redis server's monotonic clock. All replicas see the same Redis clock.

```lua
-- Current (Java clock):
local now = tonumber(ARGV[3])
local lr  = tonumber(b[2]) or now
local elapsed = math.max(0, now - lr)

-- Improved (Redis clock):
local clock = redis.call('TIME')
local now   = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local lr    = tonumber(b[2]) or now
local elapsed = math.max(0, now - lr)
```

```java
// Java side — remove now parameter
List<?> res = redisTemplate.execute(
    TOKEN_BUCKET,
    List.of("rl:" + clientId),
    String.valueOf(rule.capacity()),
    String.valueOf(rule.refillRatePerSecond()),
    "3600"   // TTL — now is computed in Lua
);
```

### Acceptance Criteria
- Zero clock skew issues across 3+ replica deployment
- No change in throughput or latency
- Correct under leap-second smearing (Redis uses monotonic system clock)

---

## 4. Rate Limit Response Headers

### Problem
The response body contains `retryAfter` as a JSON field, but well-known HTTP conventions exist. Clients must parse the body to know their rate limit status, which adds coupling.

### Solution
Add three standard headers to every response:

```java
// In RateLimitInterceptor
import jakarta.servlet.http.HttpServletResponse;

@Override
public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
    // ... existing logic ...

    RateLimitResult result = engine.checkAndConsume(clientId);

    response.setHeader("X-RateLimit-Limit",     String.valueOf(rule.capacity()));
    response.setHeader("X-RateLimit-Remaining", String.valueOf(result.remaining()));
    response.setHeader("X-RateLimit-Reset",     String.valueOf(result.retryAfter()));

    // HTTP 429 handling
    if (!result.allowed()) {
        response.setStatus(429);
        response.setHeader("Retry-After", String.valueOf(result.retryAfter()));
    }

    // ... write body ...
}
```

**Headers:**
| Header | Example | Meaning |
|--------|---------|---------|
| `X-RateLimit-Limit` | `10` | Max burst capacity |
| `X-RateLimit-Remaining` | `3` | Tokens left (may be fractional) |
| `X-RateLimit-Reset` | `1717000000` | Epoch seconds when ≥1 token available |
| `Retry-After` (429 only) | `1717000000` | Same as Reset, but HTTP-standard |

### Acceptance Criteria
- Every response includes all three `X-RateLimit-*` headers
- 429 responses include `Retry-After` header
- Verified with `curl -v`
- Existing `retryAfter` in JSON body kept for backward compat

---

## 5. Subscription Tiers from API Key Prefix

### Problem
All clients share one rate limit rule: `RateLimitRule(10, 0.16)`. In production, you need different limits per tier without adding Redis calls.

### Solution
Extract prefix from the API key (before `_`) and return the matching rule:

```java
// RateLimitRuleService.java
private static final Map<String, RateLimitRule> TIER_RULES = Map.of(
    "free", new RateLimitRule(  10,   0.16),   // 10 burst, 1/6.25s refill
    "pro",  new RateLimitRule( 100,   5.0),    // 100 burst, 5/s refill
    "ent",  new RateLimitRule(1000,  50.0)     // 1000 burst, 50/s refill
);

public RateLimitRule getRule(String clientId) {
    int idx = clientId.indexOf('_');
    if (idx != -1) {
        String tier = clientId.substring(0, idx);
        RateLimitRule rule = TIER_RULES.get(tier);
        if (rule != null) return rule;
    }
    return TIER_RULES.get("free");        // default
}
```

**API key format:** `free_sk_live_abc123`, `pro_sk_live_def456`, `ent_sk_live_ghi789`

The `clientId` stored in Prometheus metrics stays as the full key hash (never log the key itself). Tier extraction happens in-memory — zero overhead.

### Acceptance Criteria
- API key `free_*` → 10 tokens at 0.16/s
- API key `pro_*` → 100 tokens at 5/s
- API key `ent_*` → 1000 tokens at 50/s
- Unknown prefix → defaults to `free`
- No additional Redis calls

---

## 6. Global + Per-Client Dual Bucket

### Problem
A malicious user can create 1000 free-tier API keys and distribute them across 1000 VUs. Each key gets its own bucket, so each gets 10 requests. Combined throughput: 10,000 requests per refill window. No global cap.

### Solution
Add a global bucket key in the same Lua script. Check both buckets atomically.

```lua
-- KEYS[1] = "rl:<clientId>"
-- KEYS[2] = "rl:global"
local cap      = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local g_cap    = tonumber(ARGV[3])      -- global capacity
local g_rate   = tonumber(ARGV[4])      -- global refill rate
local ttl      = tonumber(ARGV[5])
local now      = tonumber(redis.call('TIME')[1])

-- Client bucket
local b   = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill')
local t   = tonumber(b[1]) or cap
local lr  = tonumber(b[2]) or now
local new_t = math.min(cap, t + math.max(0, now - lr) * rate)

-- Global bucket
local gb  = redis.call('HMGET', KEYS[2], 'tokens', 'last_refill')
local gt  = tonumber(gb[1]) or g_cap
local glr = tonumber(gb[2]) or now
local new_gt = math.min(g_cap, gt + math.max(0, now - glr) * g_rate)

local allowed = 0
if new_t >= 1 and new_gt >= 1 then
    new_t   = new_t - 1
    new_gt  = new_gt - 1
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', new_t, 'last_refill', now)
redis.call('EXPIRE', KEYS[1], ttl)
redis.call('HSET', KEYS[2], 'tokens', new_gt, 'last_refill', now)
redis.call('EXPIRE', KEYS[2], ttl)

return {allowed, math.floor(new_t), math.floor(new_gt)}
```

```java
// RateLimitEngine.java
List<?> res = redisTemplate.execute(
    TOKEN_BUCKET,
    List.of("rl:" + clientId, "rl:global"),
    String.valueOf(rule.capacity()),
    String.valueOf(rule.refillRatePerSecond()),
    String.valueOf(globalRule.capacity()),      // e.g. 5000
    String.valueOf(globalRule.refillRatePerSecond()), // e.g. 100
    "3600"
);
```

### Acceptance Criteria
- A single client cannot consume more than `global.capacity` across all their keys
- Per-client fairness preserved — one bad client doesn't starve others
- No additional round trips (single atomic script)
- Global bucket TTL matched to per-client TTL

---

## 7. Network Latency Testing with `tc netem`

### Problem
All tests run on localhost — Redis RTT is ~0.1ms. In production, Redis is a separate pod with 1-3ms RTT. The CB settings (slidingWindowSize=10, delay=5s) and the 500ms TTL cache were tuned for localhost. Real network behavior could reveal:

- CB opens unnecessarily under transient latency spikes
- p95 degrades beyond 15ms with logging + network RTT combined
- Lettuce pool exhaustion under sustained load

### Solution
Simulate realistic network latency using `tc` on the rate limiter container:

```bash
# Add 2ms ±0.5ms RTT to Redis traffic (port 6379)
tc qdisc add dev lo root netem delay 2ms 0.5ms distribution normal

# Run the full load test
k6 run k6/loadtest.js --out csv=results_network.csv

# Remove the delay
tc qdisc del dev lo root

# Compare with baseline (no delay)
```

**Measure:**
| Metric | Localhost | 2ms RTT | 5ms RTT |
|--------|-----------|---------|---------|
| p50 latency | — | — | — |
| p95 latency | — | — | — |
| p99 latency | — | — | — |
| CB open events | — | — | — |
| Redis command rate | — | — | — |

**CB tuning if needed:**
```yaml
# application.yml
resilience4j:
  circuitbreaker:
    instances:
      redisRateLimiter:
        slidingWindowSize: 20        # larger window = less sensitive
        failureRateThreshold: 80     # tolerate more latency spikes
        waitDurationInOpenState: 2s  # faster recovery
```

### Acceptance Criteria
- Test results documented with baseline comparison
- CB settings confirmed (or adjusted) for 2ms RTT
- p99 < 15ms under 5000 req/s with 2ms RTT

---

## 8. ZGC Garbage Collector

### Problem
G1GC default pauses are 5-15ms. Under 5000 req/s with per-request structured logging + Micrometer metrics + Redis call objects, allocation rate is high (~50MB/s). G1 will pause 30-100ms at that rate — exceeds the p99 budget.

### Solution
Switch to ZGC (JDK 21 stable):

```yaml
# docker-compose.yml
services:
  ratelimiter:
    image: ratelimiter:latest
    environment:
      JAVA_TOOL_OPTIONS: >-
        -XX:+UseZGC
        -XX:ConcThreads=2
        -Xmx256m
        -Xms256m
        -XX:SoftMaxHeapSize=192m
        -XX:ZUncommitDelay=30s
        -XX:+ZGenerational
```

**Why these settings:**
| Flag | Purpose |
|------|---------|
| `UseZGC` | Sub-ms max pause, regardless of heap size |
| `ZGenerational` | JDK 21+ generational ZGC — better throughput for short-lived objects (our main allocation pattern) |
| `ConcThreads=2` | Limit GC threads in constrained pod (2 CPU) |
| `SoftMaxHeapSize=192m` | GC tries to stay below this before expanding to Xmx |
| `Xmx256m` | Generous for rate limiter (no large caches) |

### Acceptance Criteria
- GC pause max < 1ms on JFR recording
- Throughput unaffected (ZGC < 2% overhead vs G1)
- No increased memory footprint (ZGC uses more native memory but same heap)

---

## 9. Kafka Audit Log for Rate Limit Decisions

### Problem
Structured logs → Loki are great for dashboards and debugging, but:
- Loki has limited retention (typically 7-30 days)
- Cannot replay historical decisions for billing reconciliation
- Log parsers break if JSON schema changes
- No support for exactly-once downstream processing

### Solution
Produce a compact Avro record to a Kafka topic. Separate observability (Loki) from audit (Kafka).

**Avro schema** (`RateLimitDecision.avsc`):
```json
{
  "type": "record",
  "name": "RateLimitDecision",
  "namespace": "com.v.ratelimiter.audit",
  "fields": [
    {"name": "clientId",       "type": "string"},
    {"name": "allowed",        "type": "boolean"},
    {"name": "remaining",      "type": "int"},
    {"name": "retryAfter",     "type": "long"},
    {"name": "ruleCapacity",   "type": "int"},
    {"name": "ruleRefillRate", "type": "double"},
    {"name": "timestamp",      "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "requestPath",    "type": "string"},
    {"name": "httpMethod",     "type": "string"},
    {"name": "tier",           "type": ["null", "string"], "default": null}
  ]
}
```

**Spring Kafka producer:**
```java
// In RateLimitInterceptor
@Component
public class RateLimitAuditProducer {

    private final KafkaTemplate<String, RateLimitDecision> kafkaTemplate;

    public void send(RateLimitDecision decision) {
        kafkaTemplate.send("rate-limit-decisions", decision.getClientId(), decision);
    }
}
```

**Downstream consumers:**
| Consumer | Purpose |
|----------|---------|
| Spark structured streaming | Hourly billing aggregates (allowed count per client) |
| FlinkCEP | Anomaly detection — sudden rejection spikes |
| ClickHouse | Long-term analytics (6mo+ retention) |
| Kafka Connect → S3 | Cold storage backup |

**Logging is kept as-is.** The Kafka producer is fire-and-forget with `acks=1` — never blocks the hot path. If Kafka is down, decisions continue (only logging + metrics); the audit stream lags.

### Acceptance Criteria
- Kafka cluster available (can start with Redpanda single-node for dev)
- Produce 5000 msgs/s with < 0.5ms p99 overhead (async producer)
- Avro schema registered in Schema Registry
- Spark structured streaming job consumes and writes hourly aggregates to PostgreSQL

---

## 10. `Retry-After` as HTTP Header for 429 Responses

### Problem
When a client is rate-limited, they receive a JSON body with `retryAfter`. But:
- Standard HTTP bots (Googlebot, Bingbot) only respect the `Retry-After` header, not the body
- CDNs (Cloudflare, Fastly) can use `Retry-After` to cache 429 responses
- Proxy servers can delay retries without parsing the body

### Solution
Set `Retry-After` as an HTTP header on 429 responses. Use **`HttpDate` format** (preferred over delta-seconds for proxies):

```java
// In RateLimitInterceptor
import java.time.Instant;
import java.time.format.DateTimeFormatter;

if (!result.allowed()) {
    response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());

    // Retry-After as HTTP-date (RFC 7231)
    String retryDate = DateTimeFormatter.RFC_1123_DATE_TIME
        .format(Instant.ofEpochSecond(result.retryAfter()));
    response.setHeader("Retry-After", retryDate);

    // Also add Retry-After as delta-seconds for simplicity
    long now = System.currentTimeMillis() / 1000;
    long delta = Math.max(1, result.retryAfter() - now);
    response.setHeader("Retry-After-Delta", String.valueOf(delta));
}
```

**Example response:**
```
HTTP/1.1 429 Too Many Requests
Content-Type: application/json
Retry-After: Wed, 27 May 2026 14:30:00 GMT
X-RateLimit-Limit: 10
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1776436200

{"allowed":false,"remaining":0,"retryAfter":1776436200}
```

### Acceptance Criteria
- All 429 responses include `Retry-After` header in RFC 1123 format
- `curl -v` shows the header
- Automated retry script reads `Retry-After` and waits before re-issuing
- Existing JSON `retryAfter` kept for backward compat

---

## Migration Priority

### Phase 1 — Low Effort, High Impact (Week 1)
| # | Task | Days |
|---|------|------|
| 8 | ZGC flags → add to Dockerfile, verify JFR | 0.5 |
| 5 | Subscription tiers → `Map<String, RateLimitRule>` | 0.5 |
| 4 | Response headers → `X-RateLimit-*` in interceptor | 0.5 |
| 10 | `Retry-After` header on 429 | 0.5 |
| 7 | `tc netem` test → document baseline vs 2ms RTT | 1 |

### Phase 2 — Core Engine Changes (Week 2)
| # | Task | Days |
|---|------|------|
| 1 | StampedLock → replace synchronized, verify correctness | 1 |
| 3 | Lua clock → use `redis.call('TIME')`, remove `now` param | 1 |
| 6 | Global + per-client Lua script, deploy, test | 2 |

### Phase 3 — Advanced (Week 3)
| # | Task | Days |
|---|------|------|
| 2 | Request collapsing → CompletableFuture on LocalBucket | 2 |
| 9 | Kafka producer + Avro schema + Spark consumer | 3 |
