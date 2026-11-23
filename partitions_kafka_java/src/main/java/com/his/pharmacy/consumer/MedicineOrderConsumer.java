package com.his.pharmacy.consumer;

import com.his.pharmacy.dto.MedicineOrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class MedicineOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(MedicineOrderConsumer.class);

    // Consumer Group 1: Pharmacy Dispensing Team
    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            dltTopicSuffix = ".dlt"
    )
    @KafkaListener(
            topics = "${app.kafka.topics.medicine-orders}",
            groupId = "his-pharmacy-dispensing-group",
            concurrency = "${app.kafka.consumer.concurrency:6}"
    )
    public void handleMedicineOrder(
            MedicineOrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[Pharmacy Group] Consuming [thread={}, partition={}, offset={}] patientId={} | status={}",
                Thread.currentThread().getName(), partition, offset, event.patientId(), event.status());
    }

    // Consumer Group 2: Hospital Billing Team (reads the SAME topic independently!)
    @KafkaListener(
            topics = "${app.kafka.topics.medicine-orders}",
            groupId = "his-billing-group",
            concurrency = "2"
    )
    public void handleBilling(MedicineOrderEvent event) {
        log.info("[Billing Group] Creating invoice for patientId={} | orderId={} | medicine={}",
                event.patientId(), event.orderId(), event.medicineName());
    }

    @DltHandler
    public void handleDeadLetter(
            MedicineOrderEvent failedEvent,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {
        log.error("Sent to DLT! patientId={} | eventId={} | reason={}",
                failedEvent.patientId(), failedEvent.eventId(), errorMessage);
    }
}
