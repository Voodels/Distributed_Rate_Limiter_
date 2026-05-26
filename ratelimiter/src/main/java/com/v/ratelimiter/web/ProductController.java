package com.v.ratelimiter.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/v1/products")
public class ProductController {

    @GetMapping
    public ResponseEntity<List<String>> getProducts() {

        List<String> products = List.of(
                "Wireless Headphones",
                "Mechanical Keyboard",
                "Ergonomic Mouse",
                "4K Monitor"
        );

        log.info("GET /api/v1/products → {} items, thread={}", products.size(), Thread.currentThread());

        return ResponseEntity.ok(products);
    }
}