# Lecture 1: Introduction to Big Data

### Course Topics

- Principles of Big Data
- Batch Processing Systems for Big Data
  - Hadoop
  - Spark
- Big SQL Systems
  - Hive
  - Impala
  - Spark Data Frames/SQL
- Big Stream Processing
  - Storm
  - Spark Streaming
  - Flink

Shortcuts:
SMP -> Symmetric Multi-Processing (shared everything)
SAN -> Storage Area Network
NAS -> Network Attached Storage
CPU -> Central Processing Unit
HDFS -> Hadoop Distributed File System

MPP -> Massively Parallel Processing (shared nothing)
DAS -> Direct Attached Storage

### DB kinds

- Relational DB -> SQL
- NoSQL DB -> Not Only SQL
  - Key-Value Store
  - Document Store
  - Column Store
  - Graph Store
- Distributed Processing DB -> Hadoop, Spark, Flink, Storm, etc.
- Analytical DB -> OLAP, Data Warehousing, etc.
- In-Memory Processing DB -> Redis, Memcached, etc.

# Chapter 1 - Meet Hadoop

- YARN -> Yet Another Resource Negotiator
  - YARN is a cluster manager can be managed many applications in a cluster. one of this application is MapReduce.
  - Interactive SQL -> query data in real-time questions and get answers in real-time. (Hive, Impala, Spark SQL) MapReduce is not good for this.
  - Iterative Processing -> query data and get the result as input to another query
  - Stream Processing -> process data in real-time MapReduce is not good for this.
  - Search -> query data and get the result in real-time

Monolithic vs Microservices

- **Monolithic** -> one big application that does everything (old style) like Oracle, SQL Server, etc.
- **Microservices** -> many small applications that do one thing well (new style) like Hadoop, Spark, etc.

**- Why Hadoop? Why not traditional RDBMS with many disk?**

- Seek ration improvement slower than transfer rate
- Data access patterns (Read rate vs Write rate)
- Write access in DFS vs Write Access in B-Tree
- Complement or Replacement?
- MapReduce is suitable for write once and read many times
- RDBMS is suitable for write many times
- Normalization
- Still blurring

# Installation of Hadoop

1- Installation modes

- Standalone (Single Node) and all components are running on the same machine (JVM)
- Pseudo-Distributed (Development) and each component is running on a separate machine (JVM)
- Fully-Distributed (Production)

- Hadoop Components
  - Common -> Utilities and Libraries
  - HDFS -> Hadoop Distributed File System
  - YARN -> Yet Another Resource Negotiator
  - MapReduce -> Distributed Processing Framework

2- Prerequisites

- Hadoop user
- JDK -> Java Development Kit
- SSH Server (Passwordless)

3- Hadoop Installation

- Download Hadoop
- Path and Environment Variables
- Set Environment Variables
- Configure Hadoop

4- Start the cluster
5- Access Namenode and YARN from web UI
