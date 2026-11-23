package com.his.pharmacy.dto;

import java.time.Instant;
import java.util.UUID;

public record MedicineOrderEvent(
        String eventId,
        String patientId,
        String orderId,
        String status,
        String medicineName,
        int dosageMg,
        Instant occurredAt
) {
    public static MedicineOrderEvent from(MedicineOrderRequest req) {
        return new MedicineOrderEvent(
                UUID.randomUUID().toString(),
                req.patientId().trim(),
                req.orderId().trim(),
                req.status().trim().toUpperCase(),
                req.medicineName().trim(),
                req.dosageMg(),
                Instant.now()
        );
    }
}
