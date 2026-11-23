package com.his.pharmacy.dto;

public record OrderAcceptedResponse(
        String eventId,
        String patientId,
        String status,
        int partition,
        long offset
) {}
