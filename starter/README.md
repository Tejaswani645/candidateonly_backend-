# Campaign Events HTTP Service

A high-performance, thread-safe Go HTTP service designed to ingest delivery provider webhook events and serve campaign metrics to marketers.

## Key Features

- **Robust Batch Ingestion (`POST /events`)**:
  - Element-by-element JSON streaming decoder (`json.NewDecoder`).
  - Accepts valid events while rejecting invalid items (missing required fields, malformed JSON, invalid timestamp formats) without failing the whole request.
  - Global `event_id` deduplication across requests.
  - Detailed response payload with ingestion metrics (`received`, `processed`, `duplicates`, `rejected`, `errors`).

- **Campaign Statistics (`GET /campaigns/{campaign_id}/stats`)**:
  - Aggregated counts (`sent`, `delivered`, `opened`, `clicked`).
  - Scoped unique counts (`unique_opens`, `unique_clicks`) tracking distinct contacts per campaign.
  - Calculated rates (`delivery_rate`, `open_rate`, `click_rate`).
  - Date-binned daily breakdowns in UTC (`daily_stats`).

- **Paginated Event Activity (`GET /campaigns/{campaign_id}/events`)**:
  - Event activity stream supporting `limit`, `offset`, and optional `type` filter.

- **Thread-Safe In-Memory Architecture**:
  - Uses Go's `sync.RWMutex` to guarantee race-free concurrent execution (`go test -race`).

---

## Requirements

- Go 1.22+ installed (`go version`).
- No external CGO or third-party library dependencies required.

---

## Running the Service

Start the server on port 8080:

```bash
cd starter
go run .
```

Output:
```
listening on :8080
```

---

## Running Tests

Run all unit and integration tests (including seed ingestion and concurrency tests):

```bash
cd starter
go test -v ./...
```

---

## Example Usage & API Endpoints

### 1. Ingest Events (`POST /events`)

```bash
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '[
    {"event_id": "evt_00001", "campaign_id": "cmp_summer_sale", "contact_id": "ct_001", "type": "sent", "timestamp": "2026-08-10T06:15:00Z"},
    {"event_id": "evt_00002", "campaign_id": "cmp_summer_sale", "contact_id": "ct_001", "type": "delivered", "timestamp": "2026-08-10T06:16:00Z"},
    {"event_id": "evt_00003", "campaign_id": "cmp_summer_sale", "contact_id": "ct_001", "type": "opened", "timestamp": "2026-08-10T06:20:00Z"}
  ]'
```

Response:
```json
{
  "status": "ok",
  "received": 3,
  "processed": 3,
  "duplicates": 0,
  "rejected": 0
}
```

### 2. Load Seed Data (`seed/events.json`)

```bash
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  --data-binary @seed/events.json
```

Response summary includes rejected malformed items (e.g., bad timestamp formats like `"not-a-time"`) while accepting valid events.

### 3. Get Campaign Statistics (`GET /campaigns/{campaign_id}/stats`)

```bash
curl -s http://localhost:8080/campaigns/cmp_summer_sale/stats
```

Sample Response:
```json
{
  "campaign_id": "cmp_summer_sale",
  "sent": 25,
  "delivered": 24,
  "opened": 18,
  "clicked": 5,
  "unique_opens": 15,
  "unique_clicks": 4,
  "delivery_rate": 0.96,
  "open_rate": 0.625,
  "click_rate": 0.2778,
  "daily_stats": [
    {
      "date": "2026-08-10",
      "sent": 25,
      "delivered": 24,
      "opened": 18,
      "clicked": 5
    }
  ]
}
```

### 4. Get Paginated Events (`GET /campaigns/{campaign_id}/events`)

```bash
curl -s "http://localhost:8080/campaigns/cmp_summer_sale/events?limit=5&offset=0&type=opened"
```

Sample Response:
```json
{
  "campaign_id": "cmp_summer_sale",
  "total": 18,
  "limit": 5,
  "offset": 0,
  "events": [
    {
      "event_id": "evt_00003",
      "campaign_id": "cmp_summer_sale",
      "contact_id": "ct_001",
      "type": "opened",
      "timestamp": "2026-08-10T06:20:00Z"
    }
  ]
}
```
