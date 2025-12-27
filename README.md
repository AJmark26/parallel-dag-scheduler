# Parallel DAG Scheduler (C++, System design)

A high-performance **Parallel Job Scheduler** built in C++ using a **DAG (Directed Acyclic Graph)** execution model, a **thread pool**, **retry-based fault tolerance**, and **cascading cancellation** for dependent tasks.

This project demonstrates real-world systems concepts used in distributed workflow engines such as **Apache Airflow, Luigi, Prefect, Spark DAG Scheduler**, and modern orchestration systems.

---

## 🚀 Features

### ✔️ **Parallel Execution (Thread Pool)**
Uses a pool of worker threads to execute tasks concurrently, maximizing CPU utilization.

### ✔️ **Dependency Resolution (DAG Scheduling)**
Tasks are executed only when all parent tasks complete.  
Internally uses:
- `inDegree` tracking (Kahn’s Algorithm)
- A thread-safe ready queue
- Topological execution order

### ✔️ **Fault Recovery (Retries)**
Each task:
- Has a configurable retry limit (`maxRetries`)
- Automatically requeues itself on failure
- Logs failures cleanly

### ✔️ **Cascading Cancellation**
If a task fails permanently:
- All downstream tasks are marked `CANCELLED`
- Prevents wasted computation
- Avoids deadlocks due to unresolved dependencies

### ✔️ **Cycle Detection**
Before execution, the scheduler validates the DAG:
- Detects cycles using Kahn’s algorithm  
- Abort execution if a cycle exists

### ✔️ **Execution Status Tracking**
Each task maintains:
- `TaskStatus` → `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, or `CANCELLED`
- Number of attempts
- Execution logs
