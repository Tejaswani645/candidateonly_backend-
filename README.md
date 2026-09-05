# Campaign Events — Engineering Assignment Submission

**Repository Name**: [`candidateonly_backend-`](https://github.com/Tejaswani645/candidateonly_backend-)  
**GitHub URL**: [https://github.com/Tejaswani645/candidateonly_backend-](https://github.com/Tejaswani645/candidateonly_backend-)  
**Author**: [Tejaswani645](https://github.com/Tejaswani645)

---

This repository contains the complete solution for the Relay Campaign Events engineering assignment, built in Go.

---

## 📌 Repository Overview & Deliverables

| Deliverable | File / Directory | Description |
|---|---|---|
| **Notes & Written Analysis** | [`NOTES.md`](./NOTES.md) | **Parts 1, 4, 5**: Problem interpretation, assumptions, PM questions, 100M events/day Scale Memo, Angry Marketer scenario analysis, and wrap-up sections. |
| **Part 2: HTTP Service** | [`starter/`](./starter/) | Complete thread-safe Go HTTP service with streaming batch ingestion (`POST /events`), campaign metrics (`GET /stats`), and paginated activity logs (`GET /events`). Includes unit & seed integration tests. |
| **Part 3: Debugging Exercise** | [`debugging/`](./debugging/) & [`BUGS.md`](./BUGS.md) | Fixed Go script producing **100% exact output match** with `expected_output.txt`. Bug root causes and minimal fixes documented in `BUGS.md`. |
| **AI Usage Disclosure** | [`AI_USAGE.md`](./AI_USAGE.md) | Complete breakdown of AI tools used, design decisions, rejected AI suggestions, and key learnings. |

---

## 🚀 Quick Start — Running the Project

### 1. HTTP Service (`starter/`)

```bash
# Navigate to starter directory
cd starter

# Run all automated tests (unit, seed data ingestion, concurrency)
go test -v ./...

# Start the server on port 8080
go run .
```

#### Example API Requests:
```bash
# Ingest batch of events
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  --data-binary @seed/events.json

# Fetch campaign stats
curl -s http://localhost:8080/campaigns/cmp_summer_sale/stats

# Fetch paginated activity log
curl -s "http://localhost:8080/campaigns/cmp_summer_sale/events?limit=5&offset=0"
```

For full API documentation and endpoint details, see [`starter/README.md`](./starter/README.md).

---

### 2. Debugging Program (`debugging/`)

```bash
# Navigate to debugging directory
cd debugging

# Run fixed program against ground truth input
go run . events.jsonl
```

Output matches [`debugging/expected_output.txt`](./debugging/expected_output.txt) on every run.

---

## 📋 Evaluation Summary & Key Architectural Highlights

1. **Partial Batch Resilience**: Streaming element-by-element JSON decoding (`json.NewDecoder`) isolates malformed items while processing valid events, returning an itemized `200 OK` summary so webhooks don't infinitely retry whole batches.
2. **Global Deduplication**: De-duplicates repeated `event_id`s across batch boundaries.
3. **Data-Race Free**: Protected by `sync.RWMutex` (HTTP service) and `sync/atomic` (debugging script) to pass Go race detector checks (`go test -race`).
4. **No External CGO Dependencies**: Standard-library Go implementation with sub-millisecond response times.
