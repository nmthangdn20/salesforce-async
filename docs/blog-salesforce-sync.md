# Đồng bộ Salesforce theo thời gian thực: CDC, gRPC và BullMQ

> Bài viết chia sẻ kiến trúc và những bài học thực tế khi xây dựng hệ thống mirror dữ liệu từ Salesforce sang PostgreSQL gần như real-time, xử lý gap events và đảm bảo consistency trong môi trường phân tán.

---

## 1. Bài toán: Tại sao polling không đủ tốt?

Cách đơn giản nhất để đồng bộ dữ liệu từ Salesforce là **polling**: cứ mỗi N giây lại gọi API để kiểm tra có bản ghi nào thay đổi không. Cách này hoạt động được, nhưng đi kèm nhiều vấn đề:

- **Chậm trễ**: Dữ liệu có thể lạc hậu tới hàng phút tùy interval.
- **Lãng phí tài nguyên**: Phần lớn các lần poll không có gì mới, nhưng vẫn tốn API quota.
- **Khó phát hiện xóa**: Salesforce không trả về bản ghi đã xóa qua REST API thông thường — bạn phải query `isDeleted=true` riêng.
- **Khó scale**: Khi số object tăng, số lần poll tăng tuyến tính.

Giải pháp tốt hơn là **Change Data Capture (CDC)**: để Salesforce chủ động đẩy sự kiện thay đổi về phía ta, thay vì ta phải đi hỏi.

---

## 2. Tổng quan kiến trúc

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

**Stack công nghệ:**

| Thành phần | Công nghệ |
|---|---|
| Framework | NestJS + TypeScript |
| Real-time stream | gRPC Pub/Sub API + Apache Avro |
| Async processing | BullMQ + Redis |
| REST fallback | jsforce |
| Database | PostgreSQL + Drizzle ORM |
| Multi-process coord | Redis Pub/Sub |

Ý tưởng cốt lõi là **tách biệt việc nhận event (gRPC layer) khỏi việc xử lý (async workers)**. gRPC stream chỉ nhận và enqueue, còn toàn bộ logic DB được xử lý bất đồng bộ qua BullMQ.

---

## 3. Change Data Capture với Salesforce Pub/Sub API

### CDC là gì?

CDC là cơ chế mà Salesforce tự động phát sinh sự kiện mỗi khi có thay đổi trên một Salesforce Object (Account, Contact, Lead...). Mỗi sự kiện chứa:

- **`changeType`**: `CREATE`, `UPDATE`, `DELETE`, `UNDELETE`, hoặc các loại `GAP_*`
- **`recordIds`**: Danh sách ID bản ghi bị ảnh hưởng
- **`changedFields`**: Các trường thay đổi (chỉ có trong UPDATE)
- **`commitTimestamp`**: Thời điểm thay đổi được commit

### Pub/Sub API qua gRPC

Salesforce cung cấp [Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/overview) qua gRPC, sử dụng **Apache Avro** để encode binary. Điều này giúp:

- Giảm bandwidth so với JSON
- Schema được enforce tại compile time
- Throughput cao hơn REST

```typescript
// Kết nối tới Salesforce Pub/Sub endpoint
const SALESFORCE_GRPC_ENDPOINT = 'api.pubsub.salesforce.com:7443';

// Tạo subscription với replay preset
const fetchRequest: FetchRequest = {
  topic_name: '/data/AccountChangeEvent',
  num_requested: 100,             // Batch size mỗi lần fetch
  replay_preset: ReplayPreset.LATEST,
};
stream.write(fetchRequest);
```

### Flow Control

Salesforce Pub/Sub API yêu cầu client **chủ động yêu cầu thêm events** thay vì tự động push liên tục. Ta cần theo dõi `pending_num_requested` và request thêm khi gần cạn:

```typescript
private handleFlowControl(response: FetchResponse, ...): void {
  const pendingRequested = response.pending_num_requested ?? 0;
  const threshold = numRequested * FLOW_CONTROL_THRESHOLD; // 0.5

  if (pendingRequested < threshold) {
    // Request thêm 100 events khi còn < 50 pending
    stream.write({ topic_name, num_requested: 100 });
  }
}
```

### Schema Caching

Mỗi event chứa một `schema_id`. Để decode Avro, ta cần fetch schema từ Salesforce. Tuy nhiên schema rất hiếm khi thay đổi, nên cache là bắt buộc:

```typescript
// Topic cache: 5 phút
export const TOPIC_CACHE_TTL_MS = 1000 * 60 * 5;

// Schema cache: 30 phút
export const SCHEMA_CACHE_TTL_MS = 1000 * 60 * 30;
```

Khi gặp lỗi decode (có thể do schema thay đổi), cache sẽ bị invalidate và fetch lại.

### Reconnect với Exponential Backoff

Kết nối gRPC có thể bị ngắt bất cứ lúc nào. Ta dùng exponential backoff để reconnect an toàn:

```typescript
export const RECONNECT_CONFIG = {
  maxRetries: 10,
  initialDelayMs: 1000,   // 1 giây
  maxDelayMs: 60000,      // Tối đa 60 giây
  backoffMultiplier: 2,
} as const;

// delay = min(initialDelay * 2^retryCount, maxDelay)
// → 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s...
```

Quan trọng: khi reconnect, ta luôn dùng `ReplayPreset.CUSTOM` với `lastReplayId` đã lưu để **không bỏ sót event nào**.

---

## 4. Xử lý bất đồng bộ với BullMQ

### Tại sao cần queue?

gRPC stream phải luôn responsive — không thể block chờ DB write xong. Nếu xử lý đồng bộ:

- Một DB query chậm sẽ làm tắc nghẽn toàn bộ stream
- Burst traffic (nhiều event cùng lúc) sẽ overwhelm DB
- Không retry được khi DB tạm thời không available

**Giải pháp**: enqueue event vào BullMQ, workers xử lý độc lập.

### Hai queue riêng biệt

```
CDC Queue      → CdcProcessor    → xử lý CREATE/UPDATE/DELETE/UNDELETE
Gap Queue      → GapProcessor    → xử lý gap reconciliation
```

Tách queue giúp gap xử lý không block CDC bình thường, và có thể tune concurrency độc lập.

### Idempotent Operations

Vì BullMQ có thể retry khi worker fail, mọi operation đều phải **idempotent**:

- **CREATE/UPDATE** → `upsertMany` (INSERT ... ON CONFLICT DO UPDATE)
- **DELETE** → soft delete với cột `_deleted_at` (set timestamp, không xóa row)
- **UNDELETE** → clear `_deleted_at` (restore bản ghi)

Soft delete còn có lợi: ta giữ được audit trail và hỗ trợ tính năng UNDELETE của Salesforce.

---

## 5. Vấn đề Gap Events — Thử thách thực sự

Đây là phần phức tạp nhất và cũng thú vị nhất của hệ thống.

### Gap Event là gì?

Salesforce **không đảm bảo** stream luôn liên tục. Khi có sự cố (kết nối mạng bị ngắt, buffer overflow, rate limit), thay vì các CDC event thông thường, ta nhận được **Gap Event** với `changeType` bắt đầu bằng `GAP_`:

| Gap Type | Ý nghĩa |
|---|---|
| `GAP_CREATE` | Có một hoặc nhiều CREATE bị bỏ sót |
| `GAP_UPDATE` | Có một hoặc nhiều UPDATE bị bỏ sót |
| `GAP_DELETE` | Có một hoặc nhiều DELETE bị bỏ sót |
| `GAP_UNDELETE` | Có một hoặc nhiều UNDELETE bị bỏ sót |
| `GAP_OVERFLOW` | Quá nhiều event bị bỏ sót, không thể enumerate |

Gap event không chứa dữ liệu thực — ta phải tự query lại Salesforce để reconcile.

### Phân loại Gap: NORMAL vs FULL_RESYNC

Không phải mọi gap đều nghiêm trọng như nhau. Ta dùng **threshold-based classification** dựa trên cửa sổ thời gian 5 phút:

```typescript
export const GAP_CONFIG = {
  maxGapEventsBeforeResync: 5,    // ≥ 5 gap → FULL_RESYNC
  gapEventResetWindowMs: 1000 * 60 * 5,  // Reset counter sau 5 phút
  resumeCooldownMs: 1000 * 30,    // Cooldown 30s sau resume
} as const;
```

```
Nhận gap events
      │
      ├── Có GAP_OVERFLOW? ──────────────────→ FULL_RESYNC
      │
      ├── Tổng gap trong 5 phút ≥ 5? ────────→ FULL_RESYNC
      │
      └── Tổng gap < 5 ──────────────────────→ NORMAL
                │
                ├── Có recordIds? ───→ Reconcile by ID
                └── Không có IDs? ───→ Reconcile by timestamp
```

### NORMAL Gap — Reconcile có chọn lọc

Khi gap ít (< 5 trong 5 phút), ta không cần pause stream, chỉ cần query lại những record cụ thể:

**Với recordIds** (Salesforce cung cấp trong gap event):
```typescript
// 1. Đánh dấu dirty để CDC events tới không ghi đè
await dirtyRecordService.addDirty(filename, objectName, recordIds);

// 2. Query Salesforce theo ID
const where = `Id IN ('001xx...', '001yy...')`;
await salesforceAsyncService.syncRecord(repo, connection, objectName, fields, where);

// 3. Catch-up: query thêm records có LMD > maxLMD để đóng reconciliation window
// (per Salesforce docs: so sánh LastModifiedDate của gap event và record thực tế)
const catchupWhere = `${idInClause} AND LastModifiedDate > ${maxLmd}`;
await salesforceAsyncService.syncRecord(repo, connection, objectName, fields, catchupWhere);

// 4. Xóa dirty flag
await dirtyRecordService.removeDirty(filename, objectName, recordIds);
```

**Không có recordIds** (fallback):
```typescript
// Query theo timestamp range với buffer ±2 phút
const from = new Date(fromTimestamp - 2 * 60 * 1000).toISOString();
const to   = new Date(toTimestamp   + 2 * 60 * 1000).toISOString();
const where = `LastModifiedDate >= ${from} AND LastModifiedDate <= ${to}`;

// Đánh dấu dirty toàn bộ object
await dirtyRecordService.addDirtyAll(filename, objectName);
await salesforceAsyncService.syncRecord(...);
await dirtyRecordService.removeDirtyAll(filename, objectName);
```

### FULL_RESYNC — Khi mọi thứ cần được đồng bộ lại

Khi gap quá nhiều hoặc có `GAP_OVERFLOW`, ta cần full resync:

```
1. Pause gRPC stream (lưu replayId hiện tại)
2. Mark dirty ALL records của object
3. Enqueue FULL_RESYNC job vào Gap Queue
4. GapProcessor:
   a. Query SF: LastModifiedDate >= hôm qua (UTC midnight)
   b. Upsert vào PostgreSQL
   c. Query SF: deleted records trong cùng khoảng thời gian
   d. Soft-delete các records đã xóa trong DB
   e. Remove dirty flag
   f. Signal resume stream (qua Redis Pub/Sub)
5. Stream resume từ replayId đã lưu
```

---

## 6. Ngăn chặn Race Condition với Dirty Flags

Đây là vấn đề tinh tế nhất: **khi đang reconcile, CDC event mới có thể arrive và ghi đè dữ liệu đang được đồng bộ**.

```
Timeline (không có dirty flags):
  t=0: Gap detected, bắt đầu query SF
  t=1: SF trả về dữ liệu cũ (snapshot tại t=0)
  t=2: CDC event UPDATE với dữ liệu MỚI HƠN được xử lý → DB có giá trị mới ✓
  t=3: Reconcile hoàn thành, upsert snapshot t=0 → DB bị OVERWRITE với dữ liệu cũ ✗
```

**Giải pháp: Dirty Flags trên Redis**

```typescript
// Khi bắt đầu reconcile
await dirtyRecordService.addDirty(filename, 'Account', ['001xx...']);

// Trong CdcProcessor, trước khi xử lý CDC event:
const cleanIds = await dirtyRecordService.filterDirtyIds(filename, 'Account', recordIds);

if (cleanIds.length === 0) {
  // Skip hoàn toàn — đợi reconcile xong
  return;
}

if (cleanIds.length < recordIds.length) {
  // Partial dirty: chỉ xử lý những ID sạch
  // Ví dụ: event cho [A, B, C] nhưng B đang dirty → chỉ xử lý [A, C]
}

// Khi reconcile xong
await dirtyRecordService.removeDirty(filename, 'Account', ['001xx...']);
```

**Hai mức granularity:**

| Key | Pattern | Dùng khi |
|---|---|---|
| Record-level | `sf:dirty:{file}:{object}` (Redis Set of IDs) | NORMAL gap có recordIds |
| Object-level | `sf:dirty-all:{file}:{object}` (Redis String "1") | NORMAL gap không có IDs, FULL_RESYNC |

**Safety net: TTL 1 giờ** — nếu process crash giữa chừng khi reconcile, dirty flag tự động expire sau 1 tiếng, tránh block vĩnh viễn.

```typescript
private static readonly TTL_SECONDS = 3600; // 1 giờ safety net
```

---

## 7. Stream Pause/Resume và Đa tiến trình

### Cơ chế Pause/Resume với ReplayId

Khi cần FULL_RESYNC, ta không thể để stream tiếp tục nhận event (sẽ bị bỏ qua do dirty flag, nhưng vẫn lãng phí và gây log noise). Giải pháp là **pause stream**:

```typescript
private pauseStream(streamKey: StreamKey, reason: PauseReason): void {
  // 1. Đánh dấu trạng thái PAUSED
  this.streamStates.set(streamKey, {
    state: StreamState.PAUSED,
    pauseReason: reason,
  });

  // 2. Xóa khỏi activeStreams
  this.activeStreams.delete(streamKey);

  // 3. Đóng kết nối gRPC
  stream.end();
}
```

Sau khi reconcile xong, ta resume từ **đúng vị trí đã lưu** bằng `replayId`:

```typescript
async resumeStream(tenantId: string, topicName: string): Promise<void> {
  // Dùng lastReplayId đã lưu → CUSTOM preset
  // → Salesforce replay lại tất cả events đã bỏ lỡ trong khi pause
  const options = {
    replayPreset: ReplayPreset.CUSTOM,
    replayId: subConfig.lastReplayId,
  };
  await this.subscribe(config, topicName, ..., options);
}
```

Điều này đảm bảo **zero event loss**: mọi event xảy ra trong khi pause đều được replay lại sau khi resume.

### Multi-process coordination qua Redis Pub/Sub

Trong môi trường production, gRPC subscriber và BullMQ workers thường chạy ở các process khác nhau. Khi GapProcessor (worker) hoàn thành FULL_RESYNC, nó cần báo cho gRPC subscriber (main process) resume stream.

```
Worker Process:                    Main Process:
  GapProcessor                       StreamResumeSubscriber
       │                                      │
       │ hoàn thành FULL_RESYNC               │ subscribe to Redis channel
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
// GapProcessor: sau khi FULL_RESYNC xong
const onResume = async (tenantId, topicName) => {
  await redisPublisher.publish(
    STREAM_RESUME_CHANNEL,
    JSON.stringify({ tenantId, topicName }),
  );
};

// StreamResumeSubscriber: lắng nghe signal
this.redisSubscriber.subscribe(STREAM_RESUME_CHANNEL, async (message) => {
  const { tenantId, topicName } = JSON.parse(message);
  await grpcService.resumeStream(tenantId, topicName);
});
```

**Fallback**: Nếu Redis publish thất bại sau 5 lần retry, GapProcessor sẽ thử gọi trực tiếp `grpcService.resumeStream()`. Cách này chỉ hoạt động khi cả hai chạy cùng process, nhưng là safety net hữu ích cho single-process deployment.

### Cooldown sau resume

Để tránh vòng lặp vô hạn (resume → GAP_OVERFLOW lại → pause → resume...), ta implement cooldown 30 giây sau khi resume. Trong thời gian này, gap events được xử lý mà không trigger thêm pause:

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

## 8. Xử lý đặc thù của Salesforce

### Compound Field Mapping

Một số Salesforce object có **compound fields** — trường lồng nhau. Ví dụ `Account.Name` thực ra là `{ FirstName, LastName, Salutation }`. Ta cần flatten trước khi lưu vào DB:

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

### Soft Delete với `_deleted_at`

Thay vì hard delete, ta dùng cột `_deleted_at`:

```sql
-- DELETE event
UPDATE accounts SET _deleted_at = NOW() WHERE id IN ('001xx...', '001yy...')

-- UNDELETE event
UPDATE accounts SET _deleted_at = NULL WHERE id IN ('001xx...')
```

Lợi ích:
- Audit trail đầy đủ
- Dễ phục hồi dữ liệu
- Hỗ trợ UNDELETE đúng nghĩa

### OAuth Token Auto-refresh

Trước mỗi Salesforce API call (REST), ta validate và refresh token nếu cần:

```typescript
async getSalesforceConfigValidated(filename: string) {
  const config = await this.getSalesforceConfig(filename);
  // jsforce tự động refresh token nếu cần
  const conn = this.createConnection(config);
  await conn.identity(); // trigger refresh nếu token hết hạn
  return config;
}
```

---

## 9. Đảm bảo Consistency & Reliability

### At-Least-Once Semantics

BullMQ đảm bảo job được xử lý **ít nhất một lần**. Vì vậy tất cả operations phải idempotent:

- **upsertMany**: `INSERT ... ON CONFLICT (Id) DO UPDATE SET ...`
- **softDeleteMany**: Set `_deleted_at = NOW()` (idempotent — chạy nhiều lần vẫn OK)
- **undeleteMany**: Set `_deleted_at = NULL` (idempotent)

### Batch Processing

Để tối ưu hiệu năng, ta xử lý theo batch 200 records:

```typescript
private static readonly SOQL_IN_BATCH_SIZE = 200;

for (let i = 0; i < recordIds.length; i += SOQL_IN_BATCH_SIZE) {
  const chunk = recordIds.slice(i, i + SOQL_IN_BATCH_SIZE);
  await this.salesforceAsyncService.syncRecord(repo, connection, objectName, fields,
    `Id IN (${chunk.map(id => `'${id}'`).join(',')})`,
  );
}
```

### Error Isolation với Promise.allSettled

Khi xử lý nhiều CDC events cùng lúc, một event fail không được block các event khác:

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

Khi service tắt, ta cleanup đúng thứ tự để tránh orphaned connections:

```typescript
onModuleDestroy(): void {
  this.isShuttingDown = true;
  this.cleanupReconnectTimers(); // Hủy pending reconnect timers
  this.cleanupStreams();          // Đóng tất cả gRPC streams
  this.cleanupClients();         // Close gRPC clients
  this.cleanupCaches();          // Clear in-memory caches
}
```

---

## 10. Sơ đồ luồng tổng thể

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

## 11. Kết luận và bài học rút ra

### Trade-offs đã lựa chọn

**Reliability over raw speed**: Hệ thống ưu tiên đảm bảo không mất dữ liệu hơn là tốc độ tối đa. Dirty flags, stream pause/resume, catch-up sync — tất cả đều thêm độ trễ nhỏ nhưng đảm bảo correctness.

**Eventual consistency**: DB không phải lúc nào cũng giống Salesforce 100% real-time, nhưng sẽ converge về đúng sau khi mọi event được xử lý.

### Patterns có thể tái sử dụng

1. **Dirty flags cho reconciliation** — pattern này áp dụng cho bất kỳ hệ thống nào cần sync dữ liệu mà không muốn race condition.

2. **Threshold-based escalation** — phân loại mức độ nghiêm trọng của sự cố thay vì luôn dùng cách tốn kém nhất.

3. **Stream pause/resume với replayId** — zero-loss reconciliation không chỉ dùng được cho Salesforce, mà cho bất kỳ event stream có replay capability (Kafka, Kinesis...).

4. **Redis Pub/Sub cho multi-process coordination** — đơn giản và hiệu quả hơn nhiều so với shared memory hay polling.

5. **Decoupled receiver/processor qua queue** — tách gRPC stream receiver khỏi DB writer giúp cả hai có thể scale độc lập.

### Những cải tiến tiếp theo

- **Configurable thresholds per tenant**: Hiện tại `maxGapEventsBeforeResync = 5` là global. Tenant lớn có thể cần ngưỡng khác.
- **gRPC deadline timeout**: Hiện tại `GetTopic`/`GetSchema` không có timeout, có thể hang trên mạng chậm.
- **FULL_RESYNC scope optimization**: Thay vì sync `LMD >= yesterday`, dùng chính xác time range của gap để giảm data volume.
- **Unit tests**: Các edge case (partial dirty, cooldown, catch-up sync) cần coverage tốt hơn.

---

*Bài viết dựa trên dự án thực tế sử dụng NestJS, Salesforce Pub/Sub API v1, BullMQ 5.x và PostgreSQL. Code snippets đã được simplified để minh họa.*
