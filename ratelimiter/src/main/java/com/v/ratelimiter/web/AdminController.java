package com.v.ratelimiter.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.v.ratelimiter.domain.RateLimitRule;
import com.v.ratelimiter.domain.RuleUpdateEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/admin/v1")
@RequiredArgsConstructor
public class AdminController {

    private final StringRedisTemplate redisTemplate;
    private final RedisConnectionFactory connectionFactory;
    private final ObjectMapper objectMapper;

    @GetMapping("/redis/stats")
    public ResponseEntity<Map<String, Object>> redisStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        try {
            Properties info = redisTemplate.getConnectionFactory().getConnection()
                .serverCommands().info("keyspace");
            stats.put("keyspace", info);
            info = redisTemplate.getConnectionFactory().getConnection()
                .serverCommands().info("memory");
            stats.put("memory", info);
        } catch (Exception e) {
            stats.put("error", e.getMessage());
        }
        long count = Optional.ofNullable(redisTemplate.keys("rl:*")).map(Set::size).orElse(0);
        stats.put("rate_limiter_keys", count);
        return ResponseEntity.ok(stats);
    }

    @GetMapping("/redis/keys")
    public ResponseEntity<List<Map<String, Object>>> listKeys(
            @RequestParam(defaultValue = "rl:*") String pattern) {
        List<Map<String, Object>> entries = new ArrayList<>();
        try (Cursor<byte[]> cursor = connectionFactory.getConnection()
                .scan(ScanOptions.scanOptions().match(pattern).count(100).build())) {
            while (cursor.hasNext()) {
                String key = new String(cursor.next());
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("key", key);

                Map<Object, Object> hash = redisTemplate.opsForHash().entries(key);
                entry.put("fields", hash);

                Long ttl = redisTemplate.getExpire(key);
                entry.put("ttl_seconds", ttl);

                entries.add(entry);
            }
        }
        return ResponseEntity.ok(entries);
    }

    @GetMapping("/redis/keys/{clientId}")
    public ResponseEntity<Map<String, Object>> getKey(@PathVariable String clientId) {
        String key = "rl:" + clientId;
        Map<Object, Object> hash = redisTemplate.opsForHash().entries(key);
        if (hash.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", key);
        result.put("fields", hash);
        result.put("ttl_seconds", redisTemplate.getExpire(key));
        return ResponseEntity.ok(result);
    }

    @PutMapping("/rules/{clientId}")
    public ResponseEntity<String> updateRule(
            @PathVariable String clientId,
            @RequestBody RateLimitRule rule) throws Exception {

        RuleUpdateEvent event = new RuleUpdateEvent(clientId, rule);
        String payload = objectMapper.writeValueAsString(event);
        redisTemplate.convertAndSend("rate-limit-rules", payload);
        return ResponseEntity.ok("Rule updated for: " + clientId);
    }
}
