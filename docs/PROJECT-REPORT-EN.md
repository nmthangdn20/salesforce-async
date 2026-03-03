# PROJECT REPORT: SALESFORCE-ASYNC

> **Report Date:** 03/03/2026

---

## 1. Project Overview

**salesforce-async** is a real-time data synchronization system from **Salesforce** to a local **PostgreSQL** database, leveraging **Change Data Capture (CDC)** via the Salesforce Pub/Sub API over gRPC.

**Problem Statement:**

- Changes in Salesforce need to be immediately reflected in the internal database without periodic polling
- When the connection is interrupted or Salesforce reports event gaps, data consistency must still be guaranteed
- Race conditions between real-time event processing and re-synchronization must be avoided

---

## 2. Technology Stack

| Category                 | Technology                     | Purpose                                      |
| ------------------------ | ------------------------------ | -------------------------------------------- |
| **Framework**            | NestJS + TypeScript            | Backend core                                 |
| **Salesforce Streaming** | gRPC Pub/Sub API + Apache Avro | Receive CDC events in real time              |
| **Salesforce REST**      | jsforce                        | Query data during reconciliation             |
| **Message Queue**        | BullMQ + Redis                 | Async processing, decouple producer/consumer |
| **Database**             | PostgreSQL + Drizzle ORM       | Store synchronized data                      |

---

## 3. System Architecture

```
        ┌──────────────────────────────┐
        │          Salesforce          │
        │                              │
        │  ┌────────────────────────┐  │
        │  │  Pub/Sub API (gRPC)    │  │
        │  │  CDC Event Stream      │  │
        │  └────────────────────────┘  │
        │  ┌────────────────────────┐  │
        │  │  REST API (jsforce)    │  │
        │  │  SOQL / Bulk Query     │  │
        │  └────────────────────────┘  │
        └──────────┬───────────────────┘
                   │
         CDC Events│           REST Query (when reconcile needed)
    (Avro encoded) │ ◄────────────────────────────────────┐
                   ▼                                      │
    ┌──────────────────────────────────────────────────┐  │
    │              salesforce-async (NestJS)           │  │
    │                                                  │  │
    │  ┌─────────────┐    categorize events            │  │
    │  │ gRPC Client │─────────────────────────────┐   │  │
    │  │ (Subscriber)│                             │   │  │
    │  └─────────────┘                             │   │  │
    │                                              ▼   │  │
    │  ┌───────────────────────────────────────────┐   │  │
    │  │              Redis                        │   │  │
    │  │  ┌──────────────┐  ┌──────────────────┐   │   │  │
    │  │  │  CDC Queue   │  │   Gap Queue      │   │   │  │
    │  │  │  (BullMQ)    │  │   (BullMQ)       │   │   │  │
    │  │  └──────┬───────┘  └────────┬─────────┘   │   │  │
    │  │         │                   │             │   │  │
    │  │  ┌──────▼───────────────────▼──────────┐  │   │  │
    │  │  │     Dirty Record Flags (Redis Set)  │  │   │  │
    │  │  │   + Stream Resume Signal (Pub/Sub)  │  │   │  │
    │  │  └────────────────────────────────────┘   │   │  │
    │  └───────────────────────────────────────────┘   │  │
    │                       │                          │  │
    │            BullMQ Workers processing             │  │
    │                       │                          │  │
    │  ┌────────────────────┴──────────────────────┐   │  │
    │  │  CdcProcessor   │  GapProcessor           │───┘  │
    │  └───────────────────────────────────────────┘      │
    └─────────────────────────────────────────────────┬───┘
                                                      │
                                                      ▼
                                          ┌───────────────────┐
                                          │    PostgreSQL     │
                                          │  (Internal DB)    │
                                          └───────────────────┘
```

> **Explanation:**
> Salesforce continuously pushes change events via a gRPC stream (CDC). The application receives, categorizes, and enqueues them into Redis (BullMQ). Workers process them asynchronously and write to PostgreSQL. When a gap occurs, the Worker calls back to the Salesforce REST API to fetch the complete data. Redis serves as the intermediary for both the queue and the synchronization mechanism between the stream and the reconciliation process.

---

## 4. Synchronization Mechanism

### 4.1 Normal CDC Processing Flow

```
Salesforce Pub/Sub (gRPC)
         │
         │  Receive batch of CDC events (Avro encoded)
         ▼
  ┌─────────────────┐
  │ Categorize event│
  └────────┬────────┘
           │
     ┌─────┴──────┐
     │            │
     ▼            ▼
 Continuous    Gap Events
  Events       (GAP_*)
     │
     ▼
 Enqueue to CDC Queue (BullMQ)
     │
     ▼
 CdcProcessor handles:
  ┌─────────────────────────────────────────────┐
  │ 1. Is the record "dirty"?                   │
  │    → Yes: SKIP (reconciliation in progress) │
  │    → No: continue processing                │
  │                                             │
  │ 2. Handle by event type:                    │
  │    CREATE   → INSERT into PostgreSQL        │
  │    UPDATE   → UPDATE in PostgreSQL          │
  │    DELETE   → Soft delete (_deleted_at)     │
  │    UNDELETE → Clear _deleted_at (restore)   │
  └─────────────────────────────────────────────┘
         │
         ▼
    PostgreSQL
```

> **Explanation:**
> Each CDC event from Salesforce is enqueued and processed by CdcProcessor. Before writing to the database, the processor checks whether the record is currently being reconciled (dirty). If so, the event is skipped to prevent overwriting with incorrect data.

---

### 4.2 Gap Event Handling (when Salesforce reports an event gap)

A gap occurs when Salesforce cannot guarantee stream integrity — for example, when the connection drops, the buffer overflows, or too many changes happen in a short period.

```
Gap Event received
         │
         ▼
  Count total gaps in the 5-minute window
         │
    ┌────┴──────────────────────────────┐
    │                                   │
    ▼                                   ▼
 < 5 gaps                    >= 5 gaps OR GAP_OVERFLOW
 type = NORMAL                type = FULL_RESYNC
    │                                   │
    ▼                                   ▼
 Mark dirty (Redis)            Pause gRPC stream
 → CDC events skipped          → Sync SF: LastModifiedDate
    │                              >= yesterday
    ├─── Has Record IDs?           → Reconcile deleted records
    │           │                  → Resume stream
    ▼           ▼
 Query SF    Query SF
 by ID       by timestamp
 Upsert DB   Upsert DB
    │           │
    └─────┬─────┘
          │
          ▼
  Clear dirty flag (Redis)
  → CDC events resume normal processing
```

> **Explanation:**
> The gap handling decision is made upfront based on the **cumulative gap count within a 5-minute window**. If the threshold is reached (≥ 5) or a GAP_OVERFLOW is received → immediately switch to FULL_RESYNC, pause the stream, and perform a full resync. If below the threshold → process as NORMAL: mark dirty then reconcile by ID (if available) or by timestamp (fallback).

---

### 4.3 Stream Pause / Resume (during FULL_RESYNC)

```
GapProcessor detects FULL_RESYNC
         │
         ▼
  ┌──────────────────────────┐
  │ 1. pauseStream()         │
  │    Stop receiving events │
  │    Save Replay ID        │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ 2. Sync data from SF     │
  │    SOQL: LastModifiedDate│
  │    >= yesterday          │
  │    → Upsert PostgreSQL   │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ 3. Reconcile deleted     │
  │    queryAll IsDeleted=   │
  │    true → soft-delete DB │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ 4. Publish Redis Pub/Sub │
  │    STREAM_RESUME signal  │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ 5. resumeStream()        │
  │    Reconnect gRPC        │
  │    From the saved        │
  │    Replay ID             │
  └──────────────────────────┘
```

> **Explanation:**
> During FULL_RESYNC, the gRPC stream is completely paused (no new events received). The Replay ID is saved so that after sync completes, the stream can resume from exactly where it left off without losing any events. The resume signal is published via Redis Pub/Sub to ensure it works correctly even in multi-process deployments.

---

## 5. Implemented Features

### ✅ Real-time CDC Synchronization

- Handles 4 Salesforce event types: **CREATE, UPDATE, DELETE, UNDELETE**
- Asynchronous processing via BullMQ — no blocking
- Automatic compound field mapping (e.g., Account Name → FirstName / LastName)

### ✅ Gap Event Handling (3 strategies)

- **Gap with Record IDs:** Precise query by ID, upsert per record
- **Gap without IDs:** Fallback query by timestamp range (± 2 minutes)
- **FULL_RESYNC (≥ 5 gaps or GAP_OVERFLOW):** Pause stream, full resync, resume

### ✅ Stream Pause / Resume

- Safely pause gRPC stream, save Replay ID
- Automatically resume from the exact position after sync completes
- Uses Redis Pub/Sub to coordinate between processes

### ✅ Dirty Record Tracking (race condition prevention)

- Marks records under reconciliation in Redis (Set)
- CDC events arriving mid-reconciliation → skipped, no incorrect overwrites
- 1-hour TTL — auto-released if reconciliation fails

### ✅ Soft Delete & Undelete

- Records deleted in Salesforce → marked `_deleted_at` (no physical deletion)
- Supports record restoration when Salesforce undeletes

### ✅ Automatic Schema Mapping

- Reads schema from Salesforce Describe API on startup
- Automatically creates / updates corresponding PostgreSQL tables
- Type mapping: String → VARCHAR, Number → NUMERIC, Date → TIMESTAMP, Boolean → BOOLEAN

### ✅ OAuth2 Token Auto-Refresh

- Salesforce tokens are verified and refreshed before each API call
- No manual intervention required

### ✅ Bulk Import

- Import large volumes of records via PostgreSQL upsert batch (200 records/batch)
- Used for initial load and full resync

### ✅ Multi-mode Deployment

- **Full Stack:** HTTP API + gRPC stream + Workers in a single process
- **Worker-Only:** Workers only — supports independent horizontal scaling

---

## 6. Known Limitations

| Limitation            | Detail                                                       | Mitigation                                  |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------- |
| **FULL_RESYNC scope** | Only syncs `LastModifiedDate >= yesterday`, not full history | Combined with deleted record reconciliation |

---

## 7. Roadmap / Next Steps

- [ ] Support more Salesforce Objects
- [ ] Queue monitoring dashboard (BullMQ Board)
- [ ] Alerting on persistent gap events or stream failures
- [ ] Unit tests & integration tests for processors
