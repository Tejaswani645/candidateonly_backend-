package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestIngestBatchValidationAndDeduplication(t *testing.T) {
	store := NewStore()

	payload := `[
		{"event_id": "evt_001", "campaign_id": "cmp_1", "contact_id": "ct_1", "type": "sent", "timestamp": "2026-08-10T10:00:00Z"},
		{"event_id": "evt_002", "campaign_id": "cmp_1", "contact_id": "ct_1", "type": "delivered", "timestamp": "2026-08-10T10:01:00Z"},
		{"event_id": "evt_001", "campaign_id": "cmp_1", "contact_id": "ct_1", "type": "sent", "timestamp": "2026-08-10T10:00:00Z"},
		{"event_id": "", "campaign_id": "cmp_1", "contact_id": "ct_1", "type": "sent", "timestamp": "2026-08-10T10:02:00Z"},
		{"event_id": "evt_003", "campaign_id": "cmp_1", "contact_id": "ct_1", "type": "opened", "timestamp": "not-a-timestamp"},
		{"event_id": "evt_004", "campaign_id": "cmp_1", "contact_id": "ct_1", "type": "invalid_type", "timestamp": "2026-08-10T10:03:00Z"}
	]`

	resp, err := store.IngestBytes([]byte(payload))
	if err != nil {
		t.Fatalf("IngestBytes failed: %v", err)
	}

	if resp.Received != 6 {
		t.Errorf("expected Received=6, got %d", resp.Received)
	}
	if resp.Processed != 2 {
		t.Errorf("expected Processed=2, got %d", resp.Processed)
	}
	if resp.Duplicates != 1 {
		t.Errorf("expected Duplicates=1, got %d", resp.Duplicates)
	}
	if resp.Rejected != 3 {
		t.Errorf("expected Rejected=3, got %d", resp.Rejected)
	}

	stats, found := store.GetCampaignStats("cmp_1")
	if !found {
		t.Fatalf("expected campaign cmp_1 to exist")
	}
	if stats.Sent != 1 || stats.Delivered != 1 {
		t.Errorf("unexpected stats: sent=%d, delivered=%d", stats.Sent, stats.Delivered)
	}
}

func TestHTTPHandlers(t *testing.T) {
	store := NewStore()
	mux := setupRouter(store)

	// 1. Post events
	payload := `[
		{"event_id": "evt_101", "campaign_id": "cmp_summer", "contact_id": "ct_001", "type": "sent", "timestamp": "2026-08-10T08:00:00Z"},
		{"event_id": "evt_102", "campaign_id": "cmp_summer", "contact_id": "ct_001", "type": "delivered", "timestamp": "2026-08-10T08:05:00Z"},
		{"event_id": "evt_103", "campaign_id": "cmp_summer", "contact_id": "ct_001", "type": "opened", "timestamp": "2026-08-10T09:00:00Z"},
		{"event_id": "evt_104", "campaign_id": "cmp_summer", "contact_id": "ct_002", "type": "opened", "timestamp": "2026-08-10T09:15:00Z"},
		{"event_id": "evt_105", "campaign_id": "cmp_summer", "contact_id": "ct_001", "type": "clicked", "timestamp": "2026-08-10T09:30:00Z"}
	]`

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("POST /events status = %d, want 200", w.Code)
	}

	var ingestResp IngestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &ingestResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if ingestResp.Processed != 5 {
		t.Errorf("expected 5 processed, got %d", ingestResp.Processed)
	}

	// 2. GET stats for cmp_summer
	reqStats := httptest.NewRequest(http.MethodGet, "/campaigns/cmp_summer/stats", nil)
	wStats := httptest.NewRecorder()
	mux.ServeHTTP(wStats, reqStats)

	if wStats.Code != http.StatusOK {
		t.Fatalf("GET /campaigns/cmp_summer/stats status = %d, want 200", wStats.Code)
	}

	var statsResp CampaignStatsResponse
	if err := json.Unmarshal(wStats.Body.Bytes(), &statsResp); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}

	if statsResp.Sent != 1 || statsResp.Delivered != 1 || statsResp.Opened != 2 || statsResp.Clicked != 1 {
		t.Errorf("unexpected counts: %+v", statsResp)
	}
	if statsResp.UniqueOpens != 2 {
		t.Errorf("expected UniqueOpens=2, got %d", statsResp.UniqueOpens)
	}
	if statsResp.UniqueClicks != 1 {
		t.Errorf("expected UniqueClicks=1, got %d", statsResp.UniqueClicks)
	}
	if statsResp.DeliveryRate != 1.0 {
		t.Errorf("expected DeliveryRate=1.0, got %f", statsResp.DeliveryRate)
	}
	if statsResp.OpenRate != 2.0 { // 2 unique opens / 1 delivered
		t.Errorf("expected OpenRate=2.0, got %f", statsResp.OpenRate)
	}

	// 3. GET stats for non-existent campaign
	req404 := httptest.NewRequest(http.MethodGet, "/campaigns/cmp_missing/stats", nil)
	w404 := httptest.NewRecorder()
	mux.ServeHTTP(w404, req404)
	if w404.Code != http.StatusNotFound {
		t.Errorf("expected 404 for missing campaign, got %d", w404.Code)
	}

	// 4. GET paginated events
	reqEvents := httptest.NewRequest(http.MethodGet, "/campaigns/cmp_summer/events?limit=2&offset=0", nil)
	wEvents := httptest.NewRecorder()
	mux.ServeHTTP(wEvents, reqEvents)

	if wEvents.Code != http.StatusOK {
		t.Fatalf("GET /campaigns/cmp_summer/events status = %d, want 200", wEvents.Code)
	}

	var eventsResp PaginatedEventsResponse
	if err := json.Unmarshal(wEvents.Body.Bytes(), &eventsResp); err != nil {
		t.Fatalf("failed to decode events: %v", err)
	}
	if eventsResp.Total != 5 {
		t.Errorf("expected Total=5, got %d", eventsResp.Total)
	}
	if len(eventsResp.Events) != 2 {
		t.Errorf("expected 2 events in slice, got %d", len(eventsResp.Events))
	}
}

func TestSeedEventsFile(t *testing.T) {
	seedPath := filepath.Join("seed", "events.json")
	data, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("could not read seed file %s: %v", seedPath, err)
	}

	store := NewStore()
	resp, err := store.IngestBytes(data)
	if err != nil {
		t.Fatalf("failed to ingest seed file: %v", err)
	}

	if resp.Received != 235 {
		t.Errorf("expected 235 total seed events, got %d", resp.Received)
	}
	if resp.Processed == 0 {
		t.Errorf("expected processed > 0, got %d", resp.Processed)
	}
	if resp.Rejected == 0 {
		t.Errorf("expected seed file to have rejected malformed items, got 0")
	}

	// Verify stats for active campaign in seed
	stats, found := store.GetCampaignStats("cmp_summer_sale")
	if !found {
		t.Fatalf("expected campaign cmp_summer_sale in seed data")
	}
	if stats.Sent == 0 || stats.Delivered == 0 {
		t.Errorf("cmp_summer_sale should have non-zero metrics: %+v", stats)
	}
}

func TestConcurrentIngest(t *testing.T) {
	store := NewStore()
	var wg sync.WaitGroup

	numGoroutines := 10
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := `[
				{"event_id": "evt_conc_1", "campaign_id": "cmp_conc", "contact_id": "ct_1", "type": "sent", "timestamp": "2026-08-10T10:00:00Z"},
				{"event_id": "evt_conc_2", "campaign_id": "cmp_conc", "contact_id": "ct_1", "type": "delivered", "timestamp": "2026-08-10T10:01:00Z"}
			]`
			_, _ = store.IngestBytes([]byte(payload))
		}(i)
	}
	wg.Wait()

	stats, found := store.GetCampaignStats("cmp_conc")
	if !found {
		t.Fatalf("cmp_conc missing after concurrent ingestion")
	}
	if stats.Sent != 1 || stats.Delivered != 1 {
		t.Errorf("expected exactly 1 sent and 1 delivered due to deduplication, got sent=%d delivered=%d", stats.Sent, stats.Delivered)
	}
}
