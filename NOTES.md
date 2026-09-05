# Campaign Events — Notes & Design Decisions (`NOTES.md`)

---

## Part 1 — Read and Think First

### 1. Interpretation of the Problem
Relay receives marketing events (`sent`, `delivered`, `opened`, `clicked`) from third-party delivery providers via webhooks and serves aggregate statistics (`sent`, `delivered`, `opened`, `clicked`, `unique_opens`) to a live campaign dashboard.

Two key real-world constraints dominate:
1. **Provider Retries & Duplicates**: Network glitches or slow HTTP responses cause delivery providers to retry webhooks, creating duplicate events.
2. **Out-of-Order Delivery**: Webhooks arrive asynchronously and out of chronological order (e.g., `opened` arriving before `delivered`).

The core goal is to build an ingestion service that de-duplicates incoming traffic, rejects malformed records without failing valid items in a batch, and serves consistent, accurate campaign statistics.

---

### 2. Assumptions Made
- **`event_id` Uniqueness**: A non-empty `event_id` uniquely identifies a single physical delivery event assigned by the provider. If the same `event_id` is received multiple times, only the first occurrence is counted.
- **Malformed Item Isolation**: In a batch payload of 200 events, if 1 item has invalid JSON or an unparseable timestamp, the remaining 199 valid events must still be processed. The service should return HTTP `200 OK` with an itemized breakdown of accepted, duplicate, and rejected records so providers do not infinitely retry the entire batch.
- **Timestamp Standard**: Timestamps are provided by third-party systems in UTC ISO 8601 / RFC 3339 format. Daily delivery aggregations (`daily_stats`) are bucketed strictly by the event's UTC date.
- **Campaign Scope**: `unique_opens` and `unique_clicks` represent the number of distinct contacts that opened/clicked *per campaign*. A contact opening emails in 2 separate campaigns is counted once in *each* campaign.

---

### 3. Ambiguities Noticed in the Brief
- **Duplicate Payload Mutations**: If an event arrives with the same `event_id` but a different timestamp or metadata, should it update the existing record or be completely ignored? *(Assumed: Completely ignored as an idempotent retry).*
- **Stats for Non-Existent Campaigns**: Should `GET /campaigns/{campaign_id}/stats` return `404 Not Found` or `200 OK` with zeroed counters? *(Assumed: `404 Not Found` with an informative error message).*
- **Open / Click Rates Definition**: Does `open_rate` mean `unique_opens / delivered` or `total_opens / delivered`? Does `click_rate` mean `unique_clicks / unique_opens` or `unique_clicks / delivered`? *(Assumed standard email marketing definitions: `open_rate = unique_opens / delivered`, `click_rate = unique_clicks / opened`).*

---

### 4. Questions for the Product Manager
1. **Batch Failure Semantics**: When a provider batch contains malformed events, do providers prefer a partial `200 OK` with error details or a `400 Bad Request` requiring them to fix and resend?
2. **Contact Anonymization / GDPR**: Do we need to support event deletions or contact scrub requests that retroactively update aggregate statistics?
3. **Retention & Windowing**: How long should raw event logs and daily statistics be retained in memory/database for dashboard queries?

---

### 5. Priorities and Execution Order
1. **Core Domain & Ingestion Engine**: Build a thread-safe Go event store with streaming JSON decoding and deduplication.
2. **HTTP API**: Implement `POST /events`, `GET /campaigns/{campaign_id}/stats`, and `GET /campaigns/{campaign_id}/events`.
3. **Debugging Exercise (Part 3)**: Fix all 4 bugs in `debugging/main.go` and verify exact match with `expected_output.txt`.
4. **Automated Testing**: Write unit, seed ingestion, and concurrency tests.
5. **Scale Memo & Scenario Analysis (Parts 4 & 5)**: Document architecture evolution and investigate dashboard discrepancies.

---

## Part 4 — Scale Memo (100 Million Events / Day)

### 1. What Breaks First in the Current Implementation?
- **Memory Exhaustion**: The current in-memory store keeps all events and tracking maps (`seenEvents`, `UniqueOpens`, `DailyBuckets`, `EventLog`) in RAM. At 100M events/day (~1,160 events/sec average, ~10,000 events/sec peak), RAM will overflow within hours.
- **Single Process Bottleneck**: A single Go process handling HTTP decoding, deduplication, and aggregation will hit CPU and network card limits.
- **Global Mutex Contention**: Lock contention on `sync.RWMutex` across thousands of concurrent webhook goroutines will saturate CPU and tank throughput.
- **Data Loss on Restart**: Any service restart wipes all campaign counters.

---

### 2. What Would You Change, and in What Order?

```
[ Providers / Webhooks ]
           │
           ▼
   [ Load Balancer ]
           │
           ▼
 [ Ingestion Gateway ] (Stateless, validates & pushes to queue)
           │
           ▼
   [ Apache Kafka ] (Partitioned by campaign_id)
           │
           ▼
  [ Worker Pool ] (Processes batches, de-duplicates)
      │        │
      ▼        ▼
  [ Redis ] [ ClickHouse / TimescaleDB ]
(Counters)   (Persistent Event Store)
```

1. **Decouple Ingestion from Processing (Message Queue)**: Have stateless HTTP gateway nodes validate webhook payloads and write raw events directly to an append-only queue (e.g., Apache Kafka or AWS Kinesis).
2. **Partitioning by `campaign_id`**: Partition Kafka topics by `campaign_id` so all events for a campaign are processed sequentially by dedicated worker instances, eliminating cross-campaign lock contention.
3. **Distributed Deduplication Store**: Use Redis or a Bloom Filter layer backed by DB primary keys (`event_id`) to perform fast deduplication before counter increments.
4. **Columnar / Time-Series Database**: Store immutable event logs in ClickHouse or TimescaleDB for sub-second analytical queries.
5. **Read-Replica Cache for Dashboard API**: Serve `GET /stats` from Redis cache (or pre-aggregated counters) to keep dashboard read latencies under 10ms.

---

### 3. Would the API Contract or Storage Design Change?
- **API Contract**:
  - `POST /events` changes from synchronous processing to asynchronous ingestion. The API returns `202 Accepted` immediately once events are persisted to the ingress queue.
  - An optional `batch_id` can be returned for tracking batch processing status.
- **Storage Design**:
  - In-memory Go maps are replaced by ClickHouse (for analytical event storage) and Redis (for real-time counter incrementing).

---

### 4. Would You Introduce a Queue? What New Problems Does It Create?
- **Yes**, a message queue (Kafka/Kinesis) is essential to buffer traffic spikes (e.g., Black Friday email blasts).
- **New Problems Created**:
  - **At-Least-Once Delivery**: Queues guarantee delivery, but workers may receive the same message twice during rebalances or worker crashes.
  - **Out-of-Order Processing**: Multi-partition consumers can process events out of order.
  - **Consumer Lag**: High queue lag during traffic spikes means dashboard stats become eventually consistent (lagging by seconds or minutes).
  - **Dead-Letter Queues (DLQ)**: Poison pill messages need isolated handling.

---

### 5. Where Can Duplicates Now Sneak In?
1. **Delivery Provider Retries**: Providers retry webhooks on network timeouts.
2. **Queue Publisher Retries**: Ingestion gateways retry publishing to Kafka when network blips occur.
3. **Queue Consumer Rebalances**: Kafka consumers crash mid-batch, causing another worker to re-read and re-process uncommitted offsets.

---

### 6. How Would You Keep Counters Trustworthy?
- **Idempotent DB Upserts / Transactions**:
  Use `INSERT INTO events (event_id, ...) VALUES (...) ON CONFLICT (event_id) DO NOTHING`.
  Counters are updated only when a new row is inserted (`rows_affected == 1`).
- **Distributed Lock / Redis Set**:
  Use `SET event_id 1 NX EX 86400` in Redis to ensure atomic, single-execution deduplication.
- **Batch Materialized Views**:
  Run periodic background reconciliation jobs (e.g., hourly SQL `COUNT(DISTINCT contact_id)` recalculation) to repair any minor drift in pre-aggregated counters.

---

### 7. What Would You Monitor?
- **Ingestion Gateway**: HTTP request rate, latency, `202 Accepted` vs `4xx/5xx` rates.
- **Queue Health**: Kafka consumer lag, partition balance, queue byte depth, DLQ event count.
- **Worker Performance**: Processing rate (events/sec), duplicate detection rate, parse failure rate.
- **Database & Cache**: Redis hit/miss ratio, ClickHouse insertion throughput, query p99 latency.

---

### 8. What Would You Deliberately NOT Solve Yet?
- **Multi-Region Active-Active Replication**: Cross-region database sync adds extreme complexity; stick to single-region multi-AZ deployment until required.
- **Real-Time Machine Learning Bot Detection**: Automated bot filtering can run as an asynchronous batch job rather than in the hot ingestion path.
- **Long-Term Cold Data Tiering**: Retain all events in primary storage until storage limits force automated tiering to S3/Parquet.

---

## Part 5 — The Angry Marketer Scenario

### Scenario:
- **10:00 AM**: `Sent: 1,000,000` | `Delivered: 970,000` | `Opened: 250,000` | `Clicked: 20,000`
- **10:30 AM**: `Sent: 1,000,000` | `Delivered: 975,000` | `Opened: 248,000` | `Clicked: 20,000`

---

### 1. Plausible Explanations (Before Assuming Code Bug)
1. **Automated Bot / Security Scanner Filtering**:
   Enterprise security software (e.g., Barracuda, Proofpoint, Apple Mail Privacy Protection) pre-opens emails to scan links for malware. Between 10:00 and 10:30, Relay's asynchronous bot-detection pipeline identified 2,000 false opens generated by security scanners and retroactively purged them from unique campaign metrics.
2. **Contact Privacy / GDPR Scrubbing**:
   A batch GDPR deletion request or user unsubscribe processing job ran between 10:00 and 10:30, removing historical event records associated with scrubbed contacts.
3. **Dashboard Query Filter / Time Boundary Shift**:
   The dashboard UI shifted its default time filter (e.g., switching from "Today (UTC)" to "Last 24 Hours" or adjusting local browser timezone boundaries).
4. **Data Reconciliation Job Execution**:
   A scheduled 30-minute data reconciliation job identified duplicate open events that slipped through initial ingestion buffering and adjusted the aggregate table.

---

### 2. What to Check First & How to Decide
1. **Database Audit Log Query**: Query raw database audit logs for `cmp_summer_sale` between 10:00 and 10:30 for soft-deleted or updated `opened` records.
2. **Bot Detection System Logs**: Inspect whether the automated bot filtering pipeline executed a cleanup batch during that timeframe.
3. **Frontend Request Parameters**: Inspect browser network logs to verify whether the 10:30 dashboard request passed identical query parameters (time windows, timezone offsets, filters) as the 10:00 request.

---

### 3. Is `Delivered` Rising (970k -> 975k) Suspicious?
**No, delivered rising is completely normal and expected.**
Delivery providers often encounter temporary soft bounces (e.g., inbox full, rate limiting by Gmail/Outlook) and retry delivery over several hours. The 5,000 increase in `Delivered` between 10:00 and 10:30 represents successful delayed deliveries.

---

## Wrap-Up Summary

### What I Completed
- **Part 1**: Documented problem interpretation, assumptions, ambiguities, PM questions, and priorities.
- **Part 2**: Built complete, thread-safe Go HTTP service with streaming `POST /events` batch ingestion, `GET /campaigns/{campaign_id}/stats`, and `GET /campaigns/{campaign_id}/events` pagination.
- **Part 3**: Identified and fixed all 4 bugs in `debugging/main.go` (global deduplication, atomic counter increments, campaign-scoped unique opens keying, UTC date formatting). Output verified to match `expected_output.txt` exactly.
- **Part 4**: Authored comprehensive scale memo detailing 100M events/day architecture, queues, deduplication, and monitoring.
- **Part 5**: Analyzed the angry marketer scenario and documented plausible root causes.
- **Tests**: Created automated unit, seed data, and concurrency tests in `starter/main_test.go`.

### What I Intentionally Skipped
- **Persistent Disk DB Setup**: Opted for a fast, thread-safe in-memory store in `starter/` to maintain zero external CGO dependencies and instant setup (`go run .`).
- **Frontend UI Polish**: As stated in the brief, a frontend earns no extra credit.

### If I Had Another Day
1. Integrate persistent storage with SQLite (`modernc.org/sqlite` pure Go driver) or PostgreSQL.
2. Add structured JSON logging (`slog`) with correlation trace IDs (`X-Request-ID`).
3. Export Prometheus metrics (`/metrics`) for HTTP request latency and queue depth.
4. Add rate limiting middleware (`golang.org/x/time/rate`).
