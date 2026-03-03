# BÁO CÁO DỰ ÁN: SALESFORCE-ASYNC

> **Ngày báo cáo:** 03/03/2026

---

## 1. Tổng quan

**salesforce-async** là hệ thống đồng bộ dữ liệu real-time từ **Salesforce** sang **PostgreSQL** nội bộ, sử dụng cơ chế **Change Data Capture (CDC)** thông qua Salesforce Pub/Sub API (gRPC).

**Bài toán cần giải quyết:**

- Dữ liệu thay đổi trên Salesforce cần được phản chiếu ngay xuống database nội bộ mà không cần polling định kỳ
- Khi kết nối bị gián đoạn hoặc Salesforce báo có khoảng trống sự kiện (gap), dữ liệu vẫn phải được đảm bảo nhất quán
- Tránh race condition giữa việc xử lý sự kiện real-time và quá trình tái đồng bộ

---

## 2. Công nghệ sử dụng

| Nhóm                     | Công nghệ                      | Mục đích                                       |
| ------------------------ | ------------------------------ | ---------------------------------------------- |
| **Framework**            | NestJS + TypeScript            | Backend chính                                  |
| **Salesforce Streaming** | gRPC Pub/Sub API + Apache Avro | Nhận CDC events real-time                      |
| **Salesforce REST**      | jsforce                        | Query dữ liệu khi cần reconcile                |
| **Message Queue**        | BullMQ + Redis                 | Xử lý bất đồng bộ, tách biệt producer/consumer |
| **Database**             | PostgreSQL + Drizzle ORM       | Lưu trữ dữ liệu đồng bộ                        |
| **Auth**                 | OAuth2 (auto-refresh)          | Xác thực với Salesforce                        |
| **Deploy**               | Docker + docker-compose        | Đóng gói và triển khai                         |

---

## 3. Kiến trúc hệ thống

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
         CDC Events│           REST Query (khi cần reconcile)
    (Avro encoded) │ ◄────────────────────────────────────┐
                   ▼                                      │
    ┌──────────────────────────────────────────────────┐  │
    │              salesforce-async (NestJS)           │  │
    │                                                  │  │
    │  ┌─────────────┐    phân loại sự kiện            │  │
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
    │            BullMQ Workers xử lý                  │  │
    │                       │                          │  │
    │  ┌────────────────────┴──────────────────────┐   │  │
    │  │  CdcProcessor   │  GapProcessor           │───┘  │
    │  └───────────────────────────────────────────┘      │
    └─────────────────────────────────────────────────┬───┘
                                                      │
                                                      ▼
                                          ┌───────────────────┐
                                          │    PostgreSQL     │
                                          │  (Database nội bộ)│
                                          └───────────────────┘
```

> **Giải thích:**
> Salesforce liên tục đẩy sự kiện thay đổi qua gRPC stream (CDC). Ứng dụng nhận, phân loại và đưa vào hàng đợi Redis (BullMQ). Các Worker xử lý bất đồng bộ và ghi xuống PostgreSQL. Khi có gap, Worker gọi lại Salesforce REST API để lấy dữ liệu đầy đủ. Redis đóng vai trò trung gian cho cả queue lẫn cơ chế đồng bộ giữa stream và quá trình reconcile.

---

## 4. Cơ chế đồng bộ

### 4.1 Luồng xử lý CDC bình thường

```
Salesforce Pub/Sub (gRPC)
         │
         │  Nhận batch sự kiện CDC (Avro encoded)
         ▼
  ┌─────────────────┐
  │ Phân loại event │
  └────────┬────────┘
           │
     ┌─────┴──────┐
     │            │
     ▼            ▼
 Continuous    Gap Events
  Events       (GAP_*)
     │
     ▼
 Đưa vào CDC Queue (BullMQ)
     │
     ▼
 CdcProcessor xử lý:
  ┌─────────────────────────────────────────────┐
  │ 1. Kiểm tra record có đang "dirty" không?   │
  │    → Nếu có: BỎ QUA (đang reconcile)        │
  │    → Nếu không: tiếp tục xử lý             │
  │                                             │
  │ 2. Xử lý theo loại sự kiện:                 │
  │    CREATE   → INSERT vào PostgreSQL          │
  │    UPDATE   → UPDATE trong PostgreSQL        │
  │    DELETE   → Soft delete (_deleted_at)      │
  │    UNDELETE → Xóa _deleted_at (phục hồi)    │
  └─────────────────────────────────────────────┘
         │
         ▼
    PostgreSQL
```

> **Giải thích:**
> Mỗi sự kiện CDC từ Salesforce sẽ được đưa vào hàng đợi và xử lý bởi CdcProcessor. Trước khi ghi xuống database, processor kiểm tra xem record đó có đang trong quá trình tái đồng bộ (dirty) không. Nếu có, sự kiện bị bỏ qua để tránh ghi đè dữ liệu sai.

---

### 4.2 Xử lý Gap Event (khi Salesforce báo có khoảng trống)

Gap xảy ra khi Salesforce không thể đảm bảo toàn vẹn stream — ví dụ khi kết nối bị ngắt, buffer tràn, hoặc quá nhiều thay đổi trong thời gian ngắn.

```
Gap Event nhận được
         │
         ▼
  Đếm tổng số gap trong cửa sổ 5 phút
         │
    ┌────┴──────────────────────────────┐
    │                                   │
    ▼                                   ▼
 < 5 gaps                    ≥ 5 gaps HOẶC GAP_OVERFLOW
 type = NORMAL                type = FULL_RESYNC
    │                                   │
    ▼                                   ▼
 Đánh dấu dirty (Redis)        Pause gRPC stream
 → CDC events bị bỏ qua        → Sync SF: LastModifiedDate
    │                              >= hôm qua
    ├─── Có Record IDs?            → Reconcile deleted records
    │           │                  → Resume stream
    ▼           ▼
 Query SF    Query SF
 theo ID     theo timestamp
 Upsert DB   Upsert DB
    │           │
    └─────┬─────┘
          │
          ▼
  Xóa dirty flag (Redis)
  → CDC events tiếp tục bình thường
```

> **Giải thích:**
> Quyết định xử lý gap được đưa ra ngay từ đầu dựa trên **tổng số gap tích lũy trong 5 phút**. Nếu đạt ngưỡng (≥ 5) hoặc nhận GAP_OVERFLOW → chuyển thẳng sang FULL_RESYNC, dừng stream và resync toàn bộ. Nếu dưới ngưỡng → xử lý NORMAL, đánh dirty rồi reconcile theo ID (nếu có) hoặc theo timestamp (fallback).

---

### 4.3 Cơ chế Pause / Resume Stream (khi FULL_RESYNC)

```
GapProcessor phát hiện FULL_RESYNC
         │
         ▼
  ┌──────────────────────────┐
  │ 1. pauseStream()         │
  │    Dừng nhận event mới   │
  │    Lưu lại Replay ID     │
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │ 2. Sync dữ liệu từ SF    │
  │    SOQL: LastModifiedDate│
  │    >= hôm qua            │
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
  │    Kết nối lại gRPC      │
  │    Từ đúng Replay ID     │
  │    đã lưu                │
  └──────────────────────────┘
```

> **Giải thích:**
> Trong quá trình FULL_RESYNC, gRPC stream bị tạm dừng hoàn toàn (không nhận event mới). Replay ID được lưu lại để sau khi sync xong, stream có thể tiếp tục từ đúng vị trí đã dừng, không bị mất sự kiện. Tín hiệu resume được phát qua Redis Pub/Sub để đảm bảo hoạt động ngay cả khi chạy multi-process.

---

## 5. Tính năng đã hoàn thành

### ✅ Đồng bộ real-time (CDC)

- Nhận và xử lý 4 loại sự kiện Salesforce: **CREATE, UPDATE, DELETE, UNDELETE**
- Xử lý bất đồng bộ qua BullMQ — không blocking
- Tự động mapping compound fields của Salesforce (ví dụ: Account Name → FirstName / LastName)

### ✅ Xử lý Gap Event (3 chiến lược)

- **Gap có Record ID:** Query chính xác theo ID, upsert từng record
- **Gap không có ID:** Fallback query theo khoảng timestamp (± 2 phút)
- **FULL_RESYNC (≥ 5 gaps hoặc GAP_OVERFLOW):** Pause stream, resync toàn bộ, resume lại

### ✅ Pause / Resume Stream

- Dừng gRPC stream an toàn, lưu Replay ID
- Tự động resume đúng vị trí sau khi sync xong
- Dùng Redis Pub/Sub để phối hợp giữa các process

### ✅ Dirty Record Tracking (chống race condition)

- Đánh dấu record đang reconcile vào Redis (Set)
- CDC event đến đúng lúc → bỏ qua, không ghi đè dữ liệu sai
- TTL 1 giờ — tự giải phóng nếu reconcile thất bại

### ✅ Soft Delete & Undelete

- Record bị xoá trên Salesforce → đánh dấu `_deleted_at` (không xoá vật lý)
- Hỗ trợ phục hồi record khi Salesforce undelete

### ✅ Schema Mapping tự động

- Đọc schema từ Salesforce Describe API khi khởi động
- Tự tạo / cập nhật bảng PostgreSQL tương ứng
- Map kiểu dữ liệu: String → VARCHAR, Number → NUMERIC, Date → TIMESTAMP, Boolean → BOOLEAN

### ✅ OAuth2 Token Refresh tự động

- Token Salesforce được kiểm tra và refresh trước mỗi lần gọi API
- Không cần can thiệp thủ công

### ✅ Bulk Import

- Nhập lượng lớn record qua PostgreSQL upsert batch (200 record/batch)
- Dùng cho initial load và full resync

### ✅ Multi-mode Deployment

- **Full Stack:** HTTP API + gRPC stream + Workers trong 1 process
- **Worker-Only:** Chỉ chạy worker — hỗ trợ scale ngang độc lập

---

## 6. Hạn chế đã biết

| Hạn chế                 | Chi tiết                                                           | Hướng xử lý                         |
| ----------------------- | ------------------------------------------------------------------ | ----------------------------------- |
| **Phạm vi FULL_RESYNC** | Chỉ sync `LastModifiedDate >= hôm qua`, không phải toàn bộ lịch sử | Kết hợp reconcile deleted để bù đắp |

---

## 7. Hướng phát triển tiếp theo

- [ ] Hỗ trợ thêm nhiều Salesforce Object
- [ ] Dashboard monitoring queue (BullMQ Board)
- [ ] Alert khi gap hoặc stream bị lỗi kéo dài
- [ ] Unit test & integration test cho các processor
