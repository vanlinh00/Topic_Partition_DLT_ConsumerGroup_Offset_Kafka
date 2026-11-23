package com.his.pharmacy.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record MedicineOrderRequest(
        @NotBlank(message = "patientId is required for partition ordering")
        String patientId,

        @NotBlank(message = "orderId is required")
        String orderId,

        @NotBlank(message = "status is required (e.g., PRESCRIBED, APPROVED, DISPENSED)")
        String status,

        @NotBlank(message = "medicineName is required")
        String medicineName,

        @Positive(message = "dosageMg must be greater than 0")
        int dosageMg
) {}
