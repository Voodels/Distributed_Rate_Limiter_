package com.v.ratelimiter.web;

import com.v.ratelimiter.domain.RateLimitResult;
import com.v.ratelimiter.engine.RateLimitEngine;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Slf4j
@Component
@RequiredArgsConstructor
public class RateLimitInterceptor implements HandlerInterceptor {

    private final RateLimitEngine rateLimitEngine;

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws Exception {

        String clientId = extractClientId(request);
        RateLimitResult result = rateLimitEngine.checkAndConsume(clientId);

        long retryAfter = result.resetTimeEpochSeconds() - System.currentTimeMillis() / 1000;

        if (!result.allowed()) {
            response.setHeader("X-RateLimit-Remaining", "0");
            response.setHeader("Retry-After", String.valueOf(retryAfter));
            response.setStatus(429);
            response.setContentType("application/json");
            response.getWriter().write("""
                    {"error":"Rate limit exceeded","retryAfter":%d}
                    """.formatted(retryAfter));

            log.info("rate_limit clientId={} allowed=false remaining={} retryAfter={}",
                clientId, result.remaining(), retryAfter);
            return false;
        }

        response.setHeader("X-RateLimit-Remaining", String.valueOf(result.remaining()));
        response.setHeader("X-RateLimit-Reset",     String.valueOf(result.resetTimeEpochSeconds()));

        log.info("rate_limit clientId={} allowed=true remaining={}",
            clientId, result.remaining());
        return true;
    }

    // Priority: API Key > JWT user > IP address
    // Mirrors the layered strategy discussed in the article.
    private String extractClientId(HttpServletRequest request) {
        String clientId = request.getHeader("X-Client-Id");
        if (clientId != null) return "clientid:" + clientId;

        String apiKey = request.getHeader("X-API-Key");
        if (apiKey != null) return "apikey:" + apiKey;

        String auth = request.getHeader("Authorization");
        if (auth != null && auth.startsWith("Bearer ")) {
            // TODO: decode JWT and extract sub claim for real user ID
            return "user:" + auth.substring(7, Math.min(auth.length(), 20));
        }

        // Last resort — IP (be mindful of NAT / corporate proxies)
        String forwarded = request.getHeader("X-Forwarded-For");
        String ip = (forwarded != null)
                ? forwarded.split(",")[0].trim()
                : request.getRemoteAddr();
        return "ip:" + ip;
    }
}