# Bảng task: Logic xử lý Salesforce gRPC / CDC / GAP

Tài liệu này liệt kê các task cần thực hiện để đảm bảo logic xử lý đúng, tối ưu và dễ bảo trì sau khi rà soát source (Salesforce gRPC, stream, GAP, reconnect, resume).

---

## Tổng quan đánh giá

- **Luồng chính**: Subscribe → stream data → phân loại GAP vs continuous → xử lý GAP (NORMAL / FULL_RESYNC) → flow control, reconnect, resume — **đã ổn**.
- **Các case hiện tại**: Keepalive, GAP_CREATE/UPDATE/DELETE/UNDELETE, GAP_OVERFLOW, threshold, cooldown sau resume, pause/resume, reconnect với replayId — **đều có xử lý**, một số điểm cần chỉnh nhỏ hoặc tối ưu.

---

## Bảng task chi tiết

| # | Status | Mức độ | Task | Giải thích / Mô tả | Mô tả kỹ thuật / Vấn đề | File / Vị trí |
|---|--------|--------|------|--------------------|-------------------------|----------------|
| 1 | Done | **Bug** | Set `StreamState.ACTIVE` khi subscribe lần đầu | Khi subscribe lần đầu, stream đang chạy nhưng trạng thái trong bộ nhớ không được ghi là ACTIVE. Mục đích: mọi stream đang hoạt động đều có state ACTIVE để API/health check sau này đọc đúng. | Đã set `streamStates.set(streamKey, { state: StreamState.ACTIVE })` sau `setupStreamHandlers()` trong `subscribe()`. | `salesforce-grpc.service.ts` |
| 2 | Skip | **Cleanup** | Xóa comment debug trong `categorizeEvents` | Code còn sót comment dùng để test (giả lập GAP). Cần xóa để tránh nhầm lẫn và code sạch hơn. | Còn dòng comment gán `changeType = 'GAP_CREATE'` / `'GAP_OVERFLOW'` để test — nên xóa. | `salesforce-grpc.service.ts` ~ dòng 389–390 |
| 3 | Done | **Chuẩn hóa** | Chuẩn hóa `GapInfo.fromReplayId` / `toReplayId` | ReplayId trong event là number nhưng GapInfo cần string (log, API). Cần chuyển đổi rõ ràng bằng `String(...)` thay vì cast để type an toàn và dễ đọc. | Đã dùng `String(events[0].replayId)` và `String(events[events.length - 1].replayId)` thay cho cast `as unknown as string`. | `salesforce-grpc.service.ts` – `handleGapEvents` |
| 4 | Done | **Tối ưu / Race** | Tránh double resume | Nếu gọi resume hai lần liên tiếp (ví dụ UI double-click hoặc worker + subscriber cùng gửi), có thể tạo hai stream subscribe song song. Cần khóa (flag) trong lúc đang resume để chỉ một lần subscribe được thực hiện. | Đã thêm `resumingStreams: Set<StreamKey>`; nếu đang resume thì bỏ qua lần gọi thứ hai; xóa flag trong `finally`. | `salesforce-grpc.service.ts` – `resumeStream()` |
| 5 | Skip | **Cấu hình** | Cho phép cấu hình GAP (threshold, window, cooldown) | Ngưỡng GAP và cooldown đang cố định (5 events, 5 phút reset, 30s cooldown). Một số tenant/topic cần giá trị khác (ít hoặc nhiều GAP hơn). Cần cho phép cấu hình qua options hoặc env. | `GAP_CONFIG` đang hardcode (maxGapEventsBeforeResync: 5, gapEventResetWindowMs, resumeCooldownMs). Nên đưa vào `SubscriptionOptions` hoặc env để từng tenant/topic có thể tùy chỉnh. | `constants/salesforce-grpc.constants.ts`, `grpc.types.ts` (options), `salesforce-grpc.service.ts` |
| 6 | Skip | **Tối ưu** | FULL_RESYNC: dùng khoảng gap khi có thể | Khi FULL_RESYNC, hiện luôn sync từ đầu ngày hôm trước (full ngày). Nếu gap event có khoảng thời gian (from/to), nên dùng khoảng đó để sync ít dữ liệu hơn, giảm tải DB và API. | `buildGapWhereClause` cho FULL_RESYNC luôn dùng `getFullResyncSinceDate()` (từ đầu ngày hôm trước). Có thể tối ưu bằng cách dùng `fromTimestamp`/`toTimestamp` của gap event khi có để thu hẹp phạm vi sync. | `cdc-gap-handler.service.ts` – `buildGapWhereClause()` |
| 7 | Done | **Observability** | Reset `retryCount` khi reconnect thành công | Sau khi reconnect thành công, nên coi như "đã ổn định" và reset số lần retry. Lần lỗi sau sẽ backoff từ đầu thay vì tiếp tục tăng delay, tránh delay quá dài không cần thiết. | Đã reset `subConfig.retryCount = 0` ngay sau khi `subscribe()` thành công trong `reconnect()`. | `salesforce-grpc.service.ts` – `reconnect()` |
| 8 | | **Robustness** | Timeout / deadline cho GetTopic và GetSchema | Gọi GetTopic/GetSchema không giới hạn thời gian; mạng chậm hoặc Salesforce treo có thể làm process đợi vô hạn. Cần set deadline (timeout) cho gRPC để fail nhanh và có thể retry/alert. | Gọi gRPC không có deadline; mạng chậm có thể treo. Nên thêm metadata deadline (hoặc options timeout) cho `GetTopic` và `GetSchema`. | `salesforce-grpc.service.ts` – `getTopic()`, `getSchema()` |
| 9 | Done | **Maintainability** | Schema cache invalidation hoặc TTL | Schema được cache vĩnh viễn. Nếu Salesforce đổi schema (ít khi), client có thể decode sai hoặc lỗi. Cần cơ chế hết hạn (TTL) hoặc xóa cache khi gặp lỗi decode để lần sau lấy schema mới. | Đã thêm TTL cho topic cache (5 phút) và schema cache (30 phút); khi schema refresh thì `avroTypeCache.delete(schemaId)`. | `salesforce-grpc.service.ts`, `salesforce-grpc.constants.ts` |
| 10 | Done | **Consistency** | Đảm bảo `commitTimestamp` trong GapInfo luôn number | GapInfo dùng timestamp (ms) để build query. Nếu header vẫn là bigint sau parse, cần chủ động convert sang number (ms) khi gán vào GapInfo để tránh lỗi runtime hoặc query sai. | Đã chuẩn hóa: lấy raw `commitTimestamp` (bigint | number), gán `fromTimestamp`/`toTimestamp` bằng `value != null ? Number(value) : 0`. | `salesforce-grpc.service.ts` – `handleGapEvents` |
| 11 | | **Error handling** | Xử lý lỗi khi `onEvent` throw (FULL_RESYNC / NORMAL) | Khi xử lý event (CDC hoặc GAP), nếu callback onEvent ném lỗi (ví dụ add job queue fail), stream không advance replayId để có thể replay. Cần đảm bảo caller luôn catch khi add job để không ném lỗi ra ngoài. | Nếu `subscriptionConfig.onEvent(events, gapInfo)` throw, `handleStreamData` catch và không advance replayId — đúng. Caller (SalesforceAsyncService) đã dùng `.catch()` khi add job. Có thể ghi chú trong doc. | Đã ổn: `salesforce-async.service.ts` (dùng `.catch()` khi `cdcQueue.add` / `gapQueue.add`) |
| 12 | | **Docs / Log** | Ghi chú hoặc log rõ replay không advance khi process lỗi | Khi process events lỗi, replayId cố ý không advance để server có thể gửi lại. Cần log (debug) rõ trường hợp này để khi trace log dễ hiểu tại sao replayId không đổi. | Comment hiện có: "Do not advance replayId so replay can retry from same point". Có thể thêm 1 dòng log debug khi không advance replayId do lỗi để dễ trace. | `salesforce-grpc.service.ts` – trong catch của `handleStreamData` |
| 13 | | **Testing** | Unit test cho categorizeEvents (GAP_* vs continuous) | Cần test đảm bảo mọi event có changeType bắt đầu bằng `GAP_` được xếp vào gap, còn lại vào continuous. Tránh regression khi sửa logic phân loại. | Đảm bảo mọi changeType GAP_* vào gapEvents, còn lại vào continuousEvents. | Test file cho `salesforce-grpc.service` hoặc cho helper `categorizeEvents` |
| 14 | | **Testing** | Unit test cho handleGapEvents (GAP_OVERFLOW, threshold, cooldown) | Cần test các nhánh: GAP_OVERFLOW → pause + FULL_RESYNC; vượt ngưỡng gap → pause; trong cooldown sau resume → không pause; NORMAL có/không recordIds. Đảm bảo logic gap và resume ổn định. | Các nhánh: GAP_OVERFLOW, totalCount >= threshold, inResumeCooldown, NORMAL với/không recordIds. | Test file cho gap logic |
| 15 | | **Ops** | Cấu hình RECONNECT (maxRetries, backoff) qua env | Số lần retry và thời gian chờ reconnect đang cố định. Môi trường khác nhau (dev/staging/prod) có thể cần giá trị khác. Nên đọc từ env (ví dụ RECONNECT_MAX_RETRIES, RECONNECT_INITIAL_DELAY_MS) để vận hành linh hoạt. | Giống GAP_CONFIG, nên cho phép override RECONNECT_CONFIG qua env để dễ tune theo môi trường. | `salesforce-grpc.constants.ts`, đọc env khi khởi tạo |

---

## Case hiện tại đã xử lý (không cần task)

- Keepalive: không events → chỉ update replayId, không gọi processEvents.
- PAUSED stream: processEvents bỏ qua data.
- GAP_OVERFLOW: pause, gửi events + gapInfo FULL_RESYNC, onEvent + resume flow.
- GAP threshold: đếm gap events, reset theo time window; vượt ngưỡng → pause + FULL_RESYNC.
- Resume cooldown: trong khoảng `resumeCooldownMs` sau resume không pause lại (tránh loop).
- Reconnect: backoff, dùng lastReplayId (ReplayPreset.CUSTOM), giới hạn maxRetries.
- Stream end khi PAUSED: không schedule reconnect.
- Shutdown: không schedule reconnect, cleanup timers/streams/clients/caches.
- Flow control: request thêm events khi pending_num_requested dưới threshold.
- NORMAL gap: có recordIds → reconcile by ID; không có → query theo timestamp.
- Dirty list: FULL_RESYNC mark dirty-all, NORMAL mark dirty theo recordIds hoặc dirty-all.

---

## Thứ tự ưu tiên gợi ý

1. **Làm trước (bug / cleanup nhỏ):** 1, 2, 3.  
2. **Tối ưu / ổn định:** 4, 5, 6, 7.  
3. **Robustness / maintainability:** 8, 9, 10.  
4. **Docs / test / ops:** 11, 12, 13, 14, 15.

Nếu bạn muốn, tôi có thể đề xuất patch cụ thể (diff) cho từng task (ví dụ bắt đầu từ 1–3).
