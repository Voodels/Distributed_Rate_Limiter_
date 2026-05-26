package com.v.ratelimiter.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.v.ratelimiter.domain.RateLimitRule;
import com.v.ratelimiter.domain.RuleUpdateEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin/v1/rules")
@RequiredArgsConstructor
public class AdminController {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @PutMapping("/{clientId}")
    public ResponseEntity<String> updateRule(
            @PathVariable String clientId,
            @RequestBody RateLimitRule rule) throws Exception {

        RuleUpdateEvent event = new RuleUpdateEvent(clientId, rule);
        String payload = objectMapper.writeValueAsString(event);
        redisTemplate.convertAndSend("rate-limit-rules", payload);
        return ResponseEntity.ok("Rule updated for: " + clientId);
    }
}
