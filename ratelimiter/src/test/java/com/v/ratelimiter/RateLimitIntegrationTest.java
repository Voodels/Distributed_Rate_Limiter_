package com.v.ratelimiter;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest @AutoConfigureMockMvc @Testcontainers
@Disabled("Testcontainers requires a valid Docker environment which is not available in this CLI context.")
class RateLimitIntegrationTest {

    @Container
    static GenericContainer<?> redis =
        new GenericContainer<>("redis:7.2").withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProps(DynamicPropertyRegistry reg) {
        reg.add("spring.data.redis.host", redis::getHost);
        reg.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired MockMvc mvc;

    @Test
    void tenRequestsAllowed_eleventhRejected() throws Exception {
        for (int i = 0; i < 10; i++) {
            mvc.perform(get("/api/v1/products")
                    .header("X-Client-Id", "test-client"))
               .andExpect(status().isOk())
               .andExpect(header().exists("X-RateLimit-Remaining"));
        }

        mvc.perform(get("/api/v1/products")
                .header("X-Client-Id", "test-client"))
           .andExpect(status().isTooManyRequests())
           .andExpect(header().exists("Retry-After"));
    }

    @Test
    void differentClients_isolatedBuckets() throws Exception {
        for (int i = 0; i < 10; i++) {
            mvc.perform(get("/api/v1/products")
                    .header("X-Client-Id", "client-A"))
               .andExpect(status().isOk());
        }

        // client-B should still be allowed — separate bucket
        mvc.perform(get("/api/v1/products")
                .header("X-Client-Id", "client-B"))
           .andExpect(status().isOk());
    }
}
