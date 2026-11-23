# Kafka Core Concepts – Hospital Information System (HIS)

A quick, practical Q&A reference guide explaining how Apache Kafka works in a Spring Boot Hospital Information System (HIS).

---

## 1. What is a Topic?

A **topic** in Kafka is like a folder where producers push messages and consumers pull messages.

* **Example:** Imagine your system has different business domains, such as **orders**, **payments**, or **users**—each one can be its own topic (e.g., `his.pharmacy.medicine-orders.v1`).

```java
// Producer sends the medicine order to the topic using patientId as the key
kafkaTemplate.send(topicName, event.patientId(), event);
```

---

## 2. What is a Partition?

A single topic can be split into many **partitions** (like separate lanes on a highway). This helps consumers read messages faster because multiple threads or instances can pull data in parallel.

* **Example:** If an orders topic has **3 partitions** (or **6 partitions**), **3** (or **6**) consumers can read from it at the exact same time.

```java
@KafkaListener(
    topics = "${app.kafka.topics.medicine-orders}",
    groupId = "his-pharmacy-dispensing-group",
    concurrency = "6"
)
public void handleMedicineOrder(MedicineOrderEvent event) {
    // Process order safely
}
```

---

## 3. What is a Consumer Group?

When you scale up to multiple service instances, they might all pull the same message from the same partition, resulting in **duplicate data processing**. A **Consumer Group** solves this by assigning each partition to only one instance in the group (e.g., if Instance A is reading Partition 0, Instance B will automatically read Partition 1).

```java
@KafkaListener(
    topics = "${app.kafka.topics.medicine-orders}",
    groupId = "his-pharmacy-dispensing-group",
    concurrency = "${app.kafka.consumer.concurrency:6}"
)
public void handleMedicineOrder(MedicineOrderEvent event) {
    // All server instances with this group-id share the partitions
}
```

---

## 4. What is an Offset in Apache Kafka?

An **offset** is a unique ID number assigned to each message in a partition. By saving the offset of the last handled (processed) message, the system knows where to resume and doesn't need to re-handle (reprocess) old messages if a consumer restarts.

```java
// Producer side: Inspecting the assigned offset after sending
long offset = result.getRecordMetadata().offset();

// Consumer side: Reading the partition and offset headers
public void handleMedicineOrder(
        MedicineOrderEvent event,
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
        @Header(KafkaHeaders.OFFSET) long offset) {
    // Offset is committed after this method finishes without error
}
```

---

## 5. What is a Dead Letter Topic (DLT)?

A **Dead Letter Topic (DLT)** is a backup topic (`his.pharmacy.medicine-orders.v1.dlt`) for broken messages that keep failing. When Kafka retries a broken message 3 times and it still fails, Kafka moves that broken message to the DLT for manual review and keeps the main line moving for all other patients.

```java
@DltHandler
public void handleDeadLetter(
        MedicineOrderEvent failedEvent,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {
    log.error("Sent to DLT! patientId={} | eventId={} | reason={}",
            failedEvent.patientId(), failedEvent.eventId(), errorMessage);
}
```