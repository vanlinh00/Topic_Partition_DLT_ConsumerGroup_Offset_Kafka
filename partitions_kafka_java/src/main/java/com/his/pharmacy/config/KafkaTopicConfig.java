package com.his.pharmacy.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.medicine-orders}")
    private String medicineOrdersTopic;

    @Bean
    public NewTopic medicineOrdersTopic() {
        return TopicBuilder.name(medicineOrdersTopic)
                .partitions(6)
                .replicas(1) // Set to 3 in a multi-broker production cluster
                .build();
    }
}
