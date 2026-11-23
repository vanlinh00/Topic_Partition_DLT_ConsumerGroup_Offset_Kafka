package com.his.pharmacy.controller;

import com.his.pharmacy.dto.MedicineOrderRequest;
import com.his.pharmacy.dto.OrderAcceptedResponse;
import com.his.pharmacy.service.MedicineOrderProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1/medicine-orders")
public class MedicineOrderController {

    private final MedicineOrderProducerService producerService;

    public MedicineOrderController(MedicineOrderProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<OrderAcceptedResponse>> createOrderEvent(
            @Valid @RequestBody MedicineOrderRequest request) {

        return producerService.publishOrderEvent(request)
                .thenApply(response -> ResponseEntity.status(HttpStatus.ACCEPTED).body(response));
    }
}
