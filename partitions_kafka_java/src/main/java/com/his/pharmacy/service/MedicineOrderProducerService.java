package com.his.pharmacy.service;

import com.his.pharmacy.dto.MedicineOrderEvent;
import com.his.pharmacy.dto.MedicineOrderRequest;
import com.his.pharmacy.dto.OrderAcceptedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class MedicineOrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(MedicineOrderProducerService.class);

    private final KafkaTemplate<String, MedicineOrderEvent> kafkaTemplate;
    private final String topicName;

    public MedicineOrderProducerService(
            KafkaTemplate<String, MedicineOrderEvent> kafkaTemplate,
            @Value("${app.kafka.topics.medicine-orders}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public CompletableFuture<OrderAcceptedResponse> publishOrderEvent(MedicineOrderRequest request) {
        MedicineOrderEvent event = MedicineOrderEvent.from(request);

        // Key = patientId -> Kafka automatically hashes murmur2(patientId) % numPartitions
        return kafkaTemplate.send(topicName, event.patientId(), event)
                .orTimeout(5, TimeUnit.SECONDS)
                .thenApply(result -> {
                    int partition = result.getRecordMetadata().partition();
                    long offset = result.getRecordMetadata().offset();

                    log.info("Published eventId={} | patientId={} | status={} | partition={} | offset={}",
                            event.eventId(), event.patientId(), event.status(), partition, offset);

                    return new OrderAcceptedResponse(
                            event.eventId(),
                            event.patientId(),
                            event.status(),
                            partition,
                            offset
                    );
                });
    }
}
