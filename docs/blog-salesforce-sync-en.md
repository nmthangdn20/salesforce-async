# Real-time Salesforce Sync: CDC, gRPC and BullMQ

> This article shares the architecture and practical lessons learned while building a system to mirror data from Salesforce to PostgreSQL in near real-time, handling gap events and ensuring consistency in a distributed environment.

---

## 1. The Problem: Why Polling Isn't Good Enough

The simplest way to sync data from Salesforce is **polling**: every N seconds, call the API to check if any records have changed. This works, but comes with many problems:

- **Latency**: Data can be minutes stale depending on the interval.
- **Wasted resources**: Most polls return nothing new, but still consume API quota.
- **Hard to detect deletes**: Salesforce doesn't return deleted records through the standard REST API — you have to query `isDeleted=true` separately.
- **Hard to scale**: As the number of objects grows, the number of polls grows linearly.

A better solution is **Change Data Capture (CDC)**: let Salesforce proactively push change events to us, instead of us having to ask.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                     Salesforce Cloud                    │
│         (CDC Events: CREATE / UPDATE / DELETE)          │
└────────────────────────┬────────────────────────────────┘
                         │ gRPC Pub/Sub API (Avro)
                         ▼
            ┌────────────────────────┐
            │  SalesforceGrpcService │  ← NestJS Service
            │  (Stream Subscriber)   │
            └────────────┬───────────┘
                         │ categorizeEvents()
              ┌──────────┴──────────┐
              ▼                     ▼
       Continuous Events        Gap Events
              │                     │
              ▼                     ▼
        CDC Queue              Gap Queue
       (BullMQ)                (BullMQ)
              │                     │
              ▼                     ▼
       CdcProcessor           GapProcessor
              │                     │
              ▼                     ▼
         PostgreSQL          Reconcile via
         (Upsert/            Salesforce REST
          SoftDelete)        → PostgreSQL
```

**Technology Stack:**

| Component | Technology |
|---|---|
| Framework | NestJS + TypeScript |
| Real-time stream | gRPC Pub/Sub API + Apache Avro |
| Async processing | BullMQ + Redis |
| REST fallback | jsforce |
| Database | PostgreSQL + Drizzle ORM |
| Multi-process coord | Redis Pub/Sub |

The core idea is to **decouple event reception (gRPC layer) from processing (async workers)**. The gRPC stream only receives and enqueues, while all DB logic is handled asynchronously via BullMQ.

---

## 3. Change Data Capture with Salesforce Pub/Sub API

### What is CDC?

CDC is a mechanism where Salesforce automatically generates events whenever a Salesforce Object (Account, Contact, Lead...) changes. Each event contains:

- **`changeType`**: `CREATE`, `UPDATE`, `DELETE`, `UNDELETE`, or various `GAP_*` types
- **`recordIds`**: List of affected record IDs
- **`changedFields`**: Fields that changed (only in UPDATE events)
- **`commitTimestamp`**: When the change was committed

### Pub/Sub API over gRPC

Salesforce provides the [Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/overview) over gRPC, using **Apache Avro** for binary encoding. This provides:

- Reduced bandwidth compared to JSON
- Schema enforced at compile time
- Higher throughput than REST

```typescript
// Connect to Salesforce Pub/Sub endpoint
const SALESFORCE_GRPC_ENDPOINT = 'api.pubsub.salesforce.com:7443';

// Create subscription with replay preset
const fetchRequest: FetchRequest = {
  topic_name: '/data/AccountChangeEvent',
  num_requested: 100,             // Batch size per fetch
  replay_preset: ReplayPreset.LATEST,
};
stream.write(fetchRequest);
```

### Flow Control

The Salesforce Pub/Sub API requires the client to **actively request more events** rather than having them pushed continuously. We need to track `pending_num_requested` and request more when running low:

```typescript
private handleFlowControl(response: FetchResponse, ...): void {
  const pendingRequested = response.pending_num_requested ?? 0;
  const threshold = numRequested * FLOW_CONTROL_THRESHOLD; // 0.5

  if (pendingRequested < threshold) {
    // Request 100 more events when < 50 pending remain
    stream.write({ topic_name, num_requested: 100 });
  }
}
```

### Schema Caching

Each event contains a `schema_id`. To decode Avro, we need to fetch the schema from Salesforce. However, schemas rarely change, so caching is essential:

```typescript
// Topic cache: 5 minutes
export const TOPIC_CACHE_TTL_MS = 1000 * 60 * 5;

// Schema cache: 30 minutes
export const SCHEMA_CACHE_TTL_MS = 1000 * 60 * 30;
```

When a decode error occurs (possibly due to schema changes), the cache is invalidated and the schema is re-fetched.

### Reconnect with Exponential Backoff

gRPC connections can drop at any time. We use exponential backoff to reconnect safely:

```typescript
export const RECONNECT_CONFIG = {
  maxRetries: 10,
  initialDelayMs: 1000,   // 1 second
  maxDelayMs: 60000,      // Max 60 seconds
  backoffMultiplier: 2,
} as const;

// delay = min(initialDelay * 2^retryCount, maxDelay)
// → 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s...
```

Importantly, on reconnect we always use `ReplayPreset.CUSTOM` with the saved `lastReplayId` to **never miss an event**.

---

## 4. Async Processing with BullMQ

### Why Do We Need a Queue?

The gRPC stream must always be responsive — it can't block waiting for DB writes to finish. With synchronous processing:

- A slow DB query would stall the entire stream
- Burst traffic (many events at once) would overwhelm the DB
- There's no way to retry when the DB is temporarily unavailable

**Solution**: enqueue events into BullMQ, let workers process them independently.

### Two Separate Queues

```
CDC Queue      → CdcProcessor    → handles CREATE/UPDATE/DELETE/UNDELETE
Gap Queue      → GapProcessor    → handles gap reconciliation
```

Separate queues ensure gap processing doesn't block normal CDC, and concurrency can be tuned independently.

### Idempotent Operations

Since BullMQ may retry when a worker fails, all operations must be **idempotent**:

- **CREATE/UPDATE** → `upsertMany` (INSERT ... ON CONFLICT DO UPDATE)
- **DELETE** → soft delete with `_deleted_at` column (set timestamp, don't delete the row)
- **UNDELETE** → clear `_deleted_at` (restore the record)

Soft delete has an added benefit: we preserve the audit trail and support Salesforce's UNDELETE feature.

---

## 5. Gap Events — The Real Challenge

This is the most complex and interesting part of the system.

### What Are Gap Events?

Salesforce **does not guarantee** a continuous stream. When a disruption occurs (network disconnect, buffer overflow, rate limiting), instead of the usual CDC events, we receive **Gap Events** with `changeType` starting with `GAP_`:

| Gap Type | Meaning |
|---|---|
| `GAP_CREATE` | One or more CREATE events were missed |
| `GAP_UPDATE` | One or more UPDATE events were missed |
| `GAP_DELETE` | One or more DELETE events were missed |
| `GAP_UNDELETE` | One or more UNDELETE events were missed |
| `GAP_OVERFLOW` | Too many events were missed to enumerate |

Gap events contain no actual data — we must query Salesforce directly to reconcile.

### Gap Classification: NORMAL vs FULL_RESYNC

Not all gaps are equally severe. We use **threshold-based classification** based on a 5-minute time window:

```typescript
export const GAP_CONFIG = {
  maxGapEventsBeforeResync: 5,    // ≥ 5 gaps → FULL_RESYNC
  gapEventResetWindowMs: 1000 * 60 * 5,  // Reset counter after 5 minutes
  resumeCooldownMs: 1000 * 30,    // 30s cooldown after resume
} as const;
```

```
Receive gap events
      │
      ├── GAP_OVERFLOW? ──────────────────────→ FULL_RESYNC
      │
      ├── Total gaps in 5 min ≥ 5? ──────────→ FULL_RESYNC
      │
      └── Total gaps < 5 ─────────────────────→ NORMAL
                │
                ├── Has recordIds? ──→ Reconcile by ID
                └── No IDs? ─────────→ Reconcile by timestamp
```

### NORMAL Gap — Selective Reconciliation

When gaps are infrequent (< 5 in 5 minutes), we don't need to pause the stream — just re-query the specific affected records:

**With recordIds** (provided by Salesforce in the gap event):
```typescript
// 1. Mark dirty so incoming CDC events don't overwrite
await dirtyRecordService.addDirty(filename, objectName, recordIds);

// 2. Query Salesforce by ID
const where = `Id IN ('001xx...', '001yy...')`;
await salesforceAsyncService.syncRecord(repo, connection, objectName, fields, where);

// 3. Catch-up: query records with LMD > maxLMD to close the reconciliation window
// (per Salesforce docs: compare LastModifiedDate of gap event vs actual record)
const catchupWhere = `${idInClause} AND LastModifiedDate > ${maxLmd}`;
await salesforceAsyncService.syncRecord(repo, connection, objectName, fields, catchupWhere);

// 4. Remove dirty flag
await dirtyRecordService.removeDirty(filename, objectName, recordIds);
```

**Without recordIds** (fallback):
```typescript
// Query by timestamp range with ±2 minute buffer
const from = new Date(fromTimestamp - 2 * 60 * 1000).toISOString();
const to   = new Date(toTimestamp   + 2 * 60 * 1000).toISOString();
const where = `LastModifiedDate >= ${from} AND LastModifiedDate <= ${to}`;

// Mark entire object as dirty
await dirtyRecordService.addDirtyAll(filename, objectName);
await salesforceAsyncService.syncRecord(...);
await dirtyRecordService.removeDirtyAll(filename, objectName);
```

### FULL_RESYNC — When Everything Needs to Be Re-synced

When gaps are excessive or there's a `GAP_OVERFLOW`, a full resync is required:

```
1. Pause gRPC stream (save current replayId)
2. Mark ALL records of the object as dirty
3. Enqueue FULL_RESYNC job into Gap Queue
4. GapProcessor:
   a. Query SF: LastModifiedDate >= yesterday (UTC midnight)
   b. Upsert into PostgreSQL
   c. Query SF: deleted records in the same time range
   d. Soft-delete those records in DB
   e. Remove dirty flag
   f. Signal stream resume (via Redis Pub/Sub)
5. Stream resumes from saved replayId
```

---

## 6. Preventing Race Conditions with Dirty Flags

This is the subtlest problem: **while reconciling, new CDC events can arrive and overwrite data that's currently being synced**.

```
Timeline (without dirty flags):
  t=0: Gap detected, start querying SF
  t=1: SF returns old data (snapshot at t=0)
  t=2: CDC UPDATE event with NEWER data is processed → DB has new value ✓
  t=3: Reconcile completes, upsert t=0 snapshot → DB OVERWRITTEN with stale data ✗
```

**Solution: Dirty Flags in Redis**

```typescript
// When starting reconciliation
await dirtyRecordService.addDirty(filename, 'Account', ['001xx...']);

// In CdcProcessor, before processing a CDC event:
const cleanIds = await dirtyRecordService.filterDirtyIds(filename, 'Account', recordIds);

if (cleanIds.length === 0) {
  // Skip entirely — wait for reconciliation to finish
  return;
}

if (cleanIds.length < recordIds.length) {
  // Partial dirty: only process clean IDs
  // Example: event for [A, B, C] but B is dirty → only process [A, C]
}

// When reconciliation is complete
await dirtyRecordService.removeDirty(filename, 'Account', ['001xx...']);
```

**Two levels of granularity:**

| Key | Pattern | Used when |
|---|---|---|
| Record-level | `sf:dirty:{file}:{object}` (Redis Set of IDs) | NORMAL gap with recordIds |
| Object-level | `sf:dirty-all:{file}:{object}` (Redis String "1") | NORMAL gap without IDs, FULL_RESYNC |

**Safety net: 1-hour TTL** — if the process crashes mid-reconciliation, the dirty flag automatically expires after 1 hour, preventing permanent blocking.

```typescript
private static readonly TTL_SECONDS = 3600; // 1 hour safety net
```

---

## 7. Stream Pause/Resume and Multi-process Coordination

### Pause/Resume Mechanism with ReplayId

During a FULL_RESYNC, we can't let the stream continue receiving events (they'd be ignored due to dirty flags, but still waste resources and create log noise). The solution is to **pause the stream**:

```typescript
private pauseStream(streamKey: StreamKey, reason: PauseReason): void {
  // 1. Mark state as PAUSED
  this.streamStates.set(streamKey, {
    state: StreamState.PAUSED,
    pauseReason: reason,
  });

  // 2. Remove from activeStreams
  this.activeStreams.delete(streamKey);

  // 3. Close the gRPC connection
  stream.end();
}
```

After reconciliation completes, we resume from **exactly the saved position** using `replayId`:

```typescript
async resumeStream(tenantId: string, topicName: string): Promise<void> {
  // Use saved lastReplayId → CUSTOM preset
  // → Salesforce replays all events missed during the pause
  const options = {
    replayPreset: ReplayPreset.CUSTOM,
    replayId: subConfig.lastReplayId,
  };
  await this.subscribe(config, topicName, ..., options);
}
```

This guarantees **zero event loss**: every event that occurred during the pause is replayed after resuming.

### Multi-process Coordination via Redis Pub/Sub

In production, the gRPC subscriber and BullMQ workers typically run in separate processes. When GapProcessor (worker) finishes a FULL_RESYNC, it needs to signal the gRPC subscriber (main process) to resume the stream.

```
Worker Process:                    Main Process:
  GapProcessor                       StreamResumeSubscriber
       │                                      │
       │ completes FULL_RESYNC                │ subscribed to Redis channel
       │                                      │
       ├─→ Redis PUBLISH                      │
       │   "salesforce:stream:resume"  ──────→│
       │   {tenantId, topicName}              │
       │                                      ▼
       │                              grpcService.resumeStream()
       │                                      │
       │                                      ▼
       │                              Stream ACTIVE ✓
```

```typescript
// GapProcessor: after FULL_RESYNC completes
const onResume = async (tenantId, topicName) => {
  await redisPublisher.publish(
    STREAM_RESUME_CHANNEL,
    JSON.stringify({ tenantId, topicName }),
  );
};

// StreamResumeSubscriber: listening for the signal
this.redisSubscriber.subscribe(STREAM_RESUME_CHANNEL, async (message) => {
  const { tenantId, topicName } = JSON.parse(message);
  await grpcService.resumeStream(tenantId, topicName);
});
```

**Fallback**: If Redis publish fails after 5 retries, GapProcessor will attempt to call `grpcService.resumeStream()` directly. This only works when both run in the same process, but serves as a useful safety net for single-process deployments.

### Cooldown After Resume

To prevent infinite loops (resume → GAP_OVERFLOW again → pause → resume...), we implement a 30-second cooldown after resuming. During this window, gap events are processed without triggering additional pauses:

```typescript
const inResumeCooldown = Boolean(
  subConfig.resumedAt &&
  now.getTime() - subConfig.resumedAt.getTime() < GAP_CONFIG.resumeCooldownMs,
);

if (gapInfo.type === 'FULL_RESYNC' && inResumeCooldown) {
  // Deliver without pause to avoid loop
  await onEvent(events, gapInfo);
  return;
}
```

---

## 8. Salesforce-specific Handling

### Compound Field Mapping

Some Salesforce objects have **compound fields** — nested fields. For example, `Account.Name` is actually `{ FirstName, LastName, Salutation }`. We need to flatten these before storing in the DB:

```typescript
private mappingObjectToFlat(object: TObject, data: PayloadWithHeader) {
  switch (object.apiName) {
    case 'Account':
      return handleAccountMapping(data); // Name → FirstName + LastName + Salutation
    default:
      return data;
  }
}
```

### Soft Delete with `_deleted_at`

Instead of hard deletes, we use a `_deleted_at` column:

```sql
-- DELETE event
UPDATE accounts SET _deleted_at = NOW() WHERE id IN ('001xx...', '001yy...')

-- UNDELETE event
UPDATE accounts SET _deleted_at = NULL WHERE id IN ('001xx...')
```

Benefits:
- Full audit trail
- Easy data recovery
- True UNDELETE support

### OAuth Token Auto-refresh

Before each Salesforce REST API call, we validate and refresh the token if needed:

```typescript
async getSalesforceConfigValidated(filename: string) {
  const config = await this.getSalesforceConfig(filename);
  // jsforce automatically refreshes the token if needed
  const conn = this.createConnection(config);
  await conn.identity(); // triggers refresh if token is expired
  return config;
}
```

---

## 9. Ensuring Consistency & Reliability

### At-Least-Once Semantics

BullMQ guarantees jobs are processed **at least once**. Therefore all operations must be idempotent:

- **upsertMany**: `INSERT ... ON CONFLICT (Id) DO UPDATE SET ...`
- **softDeleteMany**: Set `_deleted_at = NOW()` (idempotent — running multiple times is fine)
- **undeleteMany**: Set `_deleted_at = NULL` (idempotent)

### Batch Processing

For performance, we process in batches of 200 records:

```typescript
private static readonly SOQL_IN_BATCH_SIZE = 200;

for (let i = 0; i < recordIds.length; i += SOQL_IN_BATCH_SIZE) {
  const chunk = recordIds.slice(i, i + SOQL_IN_BATCH_SIZE);
  await this.salesforceAsyncService.syncRecord(repo, connection, objectName, fields,
    `Id IN (${chunk.map(id => `'${id}'`).join(',')})`,
  );
}
```

### Error Isolation with Promise.allSettled

When processing multiple CDC events simultaneously, one event failing must not block the others:

```typescript
const results = await Promise.allSettled(
  events.map(event => this.processSingleEvent(event))
);

results.forEach((result, index) => {
  if (result.status === 'rejected') {
    this.logger.error(`Event ${index + 1}/${events.length} failed: ${result.reason}`);
  }
});
```

### Graceful Shutdown

When the service shuts down, we clean up in the right order to avoid orphaned connections:

```typescript
onModuleDestroy(): void {
  this.isShuttingDown = true;
  this.cleanupReconnectTimers(); // Cancel pending reconnect timers
  this.cleanupStreams();          // Close all gRPC streams
  this.cleanupClients();         // Close gRPC clients
  this.cleanupCaches();          // Clear in-memory caches
}
```

---

## 10. Full Flow Diagram

```
Salesforce CDC Event (Avro/gRPC)
         │
         ▼
  handleStreamData()
         │
         ├─ Keepalive? → update replayId, return
         │
         ├─ Stream PAUSED? → ignore, return
         │
         ▼
  parseEvent() [Avro decode]
         │
         ▼
  categorizeEvents()
    ├── continuousEvents → onEvent() → CDC Queue → CdcProcessor
    │                                                    │
    │                                       filterDirtyIds()
    │                                                    │
    │                                        ├── all dirty → skip
    │                                        ├── partial → process cleanIds only
    │                                        └── clean → switch(changeType)
    │                                                        ├── CREATE  → upsertMany
    │                                                        ├── UPDATE  → updateMany
    │                                                        │            (fetch missing from SF)
    │                                                        ├── DELETE  → softDeleteMany
    │                                                        └── UNDELETE → undeleteMany
    │
    └── gapEvents → handleGapEvents()
                        │
                  Count in 5min window
                        │
          ┌─────────────┴──────────────┐
          │                            │
       NORMAL                     FULL_RESYNC
    (< 5 gaps)              (≥ 5 gaps / GAP_OVERFLOW)
          │                            │
     Has recordIds?           pauseStream(save replayId)
     ├── YES: addDirty(ids)   addDirtyAll()
     │        reconcile by ID Gap Queue → GapProcessor
     │        catch-up sync       │
     │        removeDirty(ids)    ├── syncRecord (LMD >= yesterday)
     │                            ├── getDeletedRecordIds
     └── NO:  addDirtyAll()       ├── softDeleteMany
              reconcile by        ├── removeDirtyAll()
              timestamp ±2min     └── publishResume (Redis)
              removeDirtyAll()            │
                                  StreamResumeSubscriber
                                  resumeStream(replayId)
                                  → Stream ACTIVE ✓
```

---

## 11. Conclusions and Lessons Learned

### Trade-offs Made

**Reliability over raw speed**: The system prioritizes zero data loss over maximum throughput. Dirty flags, stream pause/resume, catch-up sync — all of these add small latencies but guarantee correctness.

**Eventual consistency**: The DB won't always be 100% identical to Salesforce in real-time, but it will converge to the correct state after all events are processed.

### Reusable Patterns

1. **Dirty flags for reconciliation** — this pattern applies to any system that needs to sync data without race conditions.

2. **Threshold-based escalation** — classify the severity of incidents rather than always defaulting to the most expensive approach.

3. **Stream pause/resume with replayId** — zero-loss reconciliation isn't just for Salesforce; it applies to any event stream with replay capability (Kafka, Kinesis...).

4. **Redis Pub/Sub for multi-process coordination** — simpler and more effective than shared memory or polling.

5. **Decoupled receiver/processor via queue** — separating the gRPC stream receiver from the DB writer lets both scale independently.

### Potential Improvements

- **Configurable thresholds per tenant**: Currently `maxGapEventsBeforeResync = 5` is global. Large tenants may need a different threshold.
- **gRPC deadline timeout**: Currently `GetTopic`/`GetSchema` have no timeout, which can hang on slow networks.
- **FULL_RESYNC scope optimization**: Instead of syncing `LMD >= yesterday`, use the exact gap time range to reduce data volume.
- **Unit tests**: Edge cases (partial dirty, cooldown, catch-up sync) need better test coverage.

---

*This article is based on a real project using NestJS, Salesforce Pub/Sub API v1, BullMQ 5.x and PostgreSQL. Code snippets have been simplified for illustration purposes.*
