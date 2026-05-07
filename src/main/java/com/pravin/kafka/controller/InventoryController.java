package com.pravin.kafka.controller;

import com.pravin.kafka.dto.InventoryResponse;
import com.pravin.kafka.service.InventoryService;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/inventory")
@Validated
public class InventoryController {

    private final InventoryService service;

    public InventoryController(InventoryService service) {
        this.service = service;
    }

    @GetMapping("/{productId}")
    public ResponseEntity<InventoryResponse> get(@PathVariable Long productId) {
        return ResponseEntity.ok(service.get(productId));
    }

    @PostMapping("/reserve")
    public ResponseEntity<InventoryResponse> reserve(@RequestParam @NotNull Long productId,
                                     @RequestParam @NotNull @Positive Integer quantity) {
        return ResponseEntity.ok(service.reserve(productId, quantity));
    }
}