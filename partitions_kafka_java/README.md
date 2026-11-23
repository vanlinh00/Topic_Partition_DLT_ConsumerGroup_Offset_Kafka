# Kafka Core Concepts – Hospital Information System (HIS)

A quick, practical reference guide explaining how Apache Kafka powers reliable, parallel medicine order processing in a Spring Boot Hospital Information System.

---

## 1. Topic = The Category / Filing Cabinet

* **What it is:** A named folder where related messages live (for example, `his.pharmacy.medicine-orders.v1`).
* **Hospital Analogy:** Like separate hospital cabinets for **Lab Results**, **Billing**, and **Medicine Orders**—producers put messages in the right cabinet, and consumers read the ones they need.

```java
// Producer sends the medicine order to the topic using patientId as the key
kafkaTemplate.send(topicName, event.patientId(), event);
```

---

## 2. Partition = Parallel Lanes Inside a Topic

* **What it is:** A topic split into smaller ordered logs (`Partition 0` to `Partition 5`), where each message gets a sequential ID number called an **offset** (`0, 1, 2...`).
* **Why we need it:** Splitting into **6 partitions** lets **6 workers** process orders at the same time instead of waiting in one slow line.
* **Ordering Rule:** Kafka keeps strict order **inside one partition** (not across different partitions). Using `patientId` as the message key sends all events for `PATIENT-101` to the same lane (`Partition 2`), so `1. PRESCRIBED` always runs before `2. DISPENSED`.
* **Consumer Threads (`concurrency`):** Set `concurrency = "6"` (1 thread per partition) for maximum speed. Extra threads above the partition count stay idle.

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

## 3. Dead Letter Topic (DLT) = The "Problem Desk"

* **What it is:** A backup topic (`his.pharmacy.medicine-orders.v1.dlt`) for broken messages that still fail after automatic retries.
* **Why we need it:** Without a DLT, one corrupted order blocks every patient behind it in that partition. With `@RetryableTopic`, Kafka retries 3 times, moves the broken message to `.dlt` for manual review, and keeps the main line moving.

```java
@DltHandler
public void handleDeadLetter(
        MedicineOrderEvent failedEvent,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) String errorMessage) {
    log.error("Sent to DLT! patientId={} | eventId={} | reason={}",
            failedEvent.patientId(), failedEvent.eventId(), errorMessage);
}
```

---

## 4. Consumer Group = How Multiple Servers Share the Work Without Duplicates

* **What it is:** A team of servers or threads sharing the **same `group-id`** (`his-pharmacy-dispensing-group`) to divide partitions.
* **How it works:** With **6 partitions** and **3 servers**, Kafka splits the work (`Server A: P0–1`, `Server B: P2–3`, `Server C: P4–5`). Every order runs in parallel and is processed **only once** (instead of 3 times).

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

## 5. Consumer Group Offsets = Your Crash-Recovery Bookmark

* **What it is:** A **bookmark** saving the last message number a Consumer Group finished reading in each partition.
* **How it works:** If **Server A** finishes message **#50** and crashes, Kafka remembers `group-id = #50`. When Server A restarts, it continues at **#51**—no lost orders, and no repeated messages!

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
