# Server Performance and Health Monitor
This approach utilizes Kafka and ClickHouse for event streaming and log aggregation, aiming to monitor server performance and health. The producer employs Go concurrency and mutex to monitor CPU, memory, disk space, uptime, and network metrics, generating streaming data and publishing events to the Kafka broker. On the other hand, the consumer subscribes to the Kafka topic, receives the events, and stores them in the ClickHouse time series database.

## Setup
To implement this solution, follow these steps:
- Install Kafka.
- Install ClickHouse.
- Create the "events" table in ClickHouse.

## Solution Approach
- Data Modeling
  - Utilize structs since the event data has a static structure. Structs offer   
    memory efficiency and direct field access for improved performance.
  - Leverage embedded structs for loose coupling, adhering to best practices.
  - Use structs to simplify the serialization and deserialization processes.

- Kafka Partition\
  Two options are available for Kafka partition management. In this application, option 2 was chosen to simplify the producer code, relying on Kafka's partitioner.
  - Specify the partition when sending the topic at the producer side:
     This approach provides fine-grained control over message partitioning, which is useful when specific partitioning logic or ordered processing and data locality are required.
  - Allow Kafka to determine the partition using a partitioner:
     By relying on Kafka's built-in partitioner or a custom partitioner, Kafka automatically determines the target partition for the message based on configured logic. It can evenly distribute messages across partitions using the message key or round-robin algorithms.

- Server Events Table Schema Design\  
  The events table is designed to store server events and includes an auto-incrementing id column along with the topic, timestamp, and message columns. The table is utilizing the MergeTree engine, which is optimized for efficient querying, and it is ordered by the id column to enhance query performance. This schema allows for the systematic storage and retrieval of server events.
   
## Execution
To run the solution, follow these steps:
- Start the consumer 
  go run consumer.go
- Start the producer
  go run producer.go

To exit the consumer and the producer, press Ctrl + C.

