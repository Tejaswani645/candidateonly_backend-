# Part 3 — Debugging Findings (`BUGS.md`)

This document records the four bugs found in `debugging/main.go`, explaining what each bug is, why it occurred, the minimal fix applied, and how it was verified.

---

## Bug 1: Batch-Local Deduplication Map Scope

### 1. What the bug is
The deduplication map (`seen`) was created inside `processBatch()`. As a result, deduplication only applied to events within the same 200-item batch. Duplicate `event_id`s that appeared across different batches were processed multiple times, inflating all campaign metrics (`sent`, `delivered`, `opened`, `clicked`, `unique_opens`).

### 2. Why it happens
In `processBatch(events []Event)`:
```go
seen := make(map[string]bool) // event IDs we have already processed
```
Because `seen` was re-initialized on every invocation of `processBatch()`, its scope was limited to a single batch. When the provider retried an event in a subsequent batch, `seen[ev.EventID]` returned `false`, causing the event to be counted again.

### 3. Minimal Fix
Moved `seen` to package level (`var seen = map[string]bool{}`) so that event IDs are tracked across the entire lifecycle of the file processing.

### 4. Verification
Re-ran `main.go` on `events.jsonl` and verified that total `sent`, `delivered`, `opened`, and `clicked` counts decreased to match `expected_output.txt` exactly.

---

## Bug 2: Data Race on Counter Increments in Concurrent Workers

### 1. What the bug is
`processBatch()` worker goroutines concurrently mutated the shared `*CampaignStats` fields (`Sent`, `Delivered`, `Opened`, `Clicked`) via `apply(ev)` without thread synchronization. This caused data races and non-atomic update losses under concurrency.

### 2. Why it happens
In `apply(ev Event)`:
```go
cs := stats[ev.CampaignID]
switch ev.Type {
case "sent":
    cs.Sent++
case "delivered":
    cs.Delivered++
...
```
`numWorkers` (8 goroutines) pulled jobs from the `jobs` channel in parallel. Multiple goroutines executing non-atomic `int` increments (`cs.Sent++`) on the same `*CampaignStats` instance simultaneously resulted in race conditions (detected by Go's race detector `go run -race`).

### 3. Minimal Fix
Updated `CampaignStats` counter fields (`Sent`, `Delivered`, `Opened`, `Clicked`) to `int64` and used thread-safe atomic additions (`atomic.AddInt64(&cs.Sent, 1)`, etc.) inside `apply()`.

### 4. Verification
Verified that `apply()` operations are completely thread-safe under parallel execution without needing heavy locks. Verified output consistency across multiple concurrent runs.

---

## Bug 3: Global (Unscoped) Keying for Unique Opens Tracking

### 1. What the bug is
`unique_opens` were improperly counted across campaigns. If a contact opened emails in multiple campaigns (e.g., `cmp_A` and `cmp_B`), the open in `cmp_B` was ignored because the contact was already present in the global `openedBy` map.

### 2. Why it happens
In `track(ev Event)`:
```go
var openedBy = map[string]bool{}
...
case "opened":
    if !openedBy[ev.ContactID] {
        openedBy[ev.ContactID] = true
        cs.UniqueOpens++
    }
```
The key in `openedBy` was `ev.ContactID`. Requirement #2 states: *"unique_opens = the number of distinct contacts that opened, per campaign. The same contact opening in two campaigns counts once in each."*

### 3. Minimal Fix
Updated the `openedBy` map key to combine `CampaignID` and `ContactID`:
```go
key := ev.CampaignID + ":" + ev.ContactID
if !openedBy[key] {
    openedBy[key] = true
    cs.UniqueOpens++
}
```

### 4. Verification
Verified `unique_opens` for each campaign:
- `cmp_A`: 942
- `cmp_B`: 833
- `cmp_C`: 710
- `cmp_D`: 514
- `cmp_E`: 380
All values matched `expected_output.txt` perfectly.

---

## Bug 4: Local Timezone Shift in Daily Delivery Buckets

### 1. What the bug is
Daily delivery buckets (`DailyDelivered`) grouped events by local machine time instead of UTC date. On machines located outside UTC (e.g., UTC+5:30), delivery events were bucketed into incorrect dates (e.g., `2026-08-08` instead of `2026-08-07`).

### 2. Why it happens
In `track(ev Event)`:
```go
case "delivered":
    day := ev.Timestamp.Local().Format("2006-01-02")
    cs.DailyDelivered[day]++
```
Calling `.Local()` converted UTC timestamps to system local time prior to date string formatting. Requirement #3 states: *"Daily delivered buckets use the event's UTC date. The timestamp field is UTC."*

### 3. Minimal Fix
Replaced `.Local()` with `.UTC()`:
```go
day := ev.Timestamp.UTC().Format("2006-01-02")
```

### 4. Verification
Ran `go run . events.jsonl > actual.txt` and confirmed exact string parity against `expected_output.txt`:
```
campaign=cmp_A sent=1987 delivered=1914 opened=1283 clicked=550 unique_opens=942
  2026-08-01 delivered=270
  2026-08-02 delivered=277
  2026-08-03 delivered=275
  2026-08-04 delivered=268
  2026-08-05 delivered=280
  2026-08-06 delivered=278
  2026-08-07 delivered=266
...
```
