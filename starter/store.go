package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"
)

// CampaignData holds aggregated in-memory metrics and logs for a single campaign.
type CampaignData struct {
	ID           string
	Sent         int64
	Delivered    int64
	Opened       int64
	Clicked      int64
	UniqueOpens  map[string]bool
	UniqueClicks map[string]bool
	DailyBuckets map[string]*DailyBucket
	EventLog     []Event
}

// Store is an in-memory thread-safe event storage engine.
type Store struct {
	mu         sync.RWMutex
	seenEvents map[string]bool
	campaigns  map[string]*CampaignData
}

// NewStore initializes a new event store.
func NewStore() *Store {
	return &Store{
		seenEvents: make(map[string]bool),
		campaigns:  make(map[string]*CampaignData),
	}
}

var supportedTimeFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
}

func parseTimestamp(ts string) (time.Time, error) {
	for _, fmtStr := range supportedTimeFormats {
		if t, err := time.Parse(fmtStr, ts); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp format: %s", ts)
}

// IngestBatch streams a JSON array from r, validating each item individually.
func (s *Store) IngestBatch(r io.Reader) (*IngestResponse, error) {
	dec := json.NewDecoder(r)

	// Read opening bracket '['
	t, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid JSON payload: %w", err)
	}
	delim, ok := t.(json.Delim)
	if !ok || delim != '[' {
		return nil, errors.New("invalid JSON payload: root must be a JSON array")
	}

	resp := &IngestResponse{
		Status: "ok",
		Errors: make([]IngestError, 0),
	}

	index := 0
	for dec.More() {
		index++
		resp.Received++

		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			resp.Rejected++
			resp.Errors = append(resp.Errors, IngestError{
				Index: index,
				Error: fmt.Sprintf("malformed JSON element: %v", err),
			})
			continue
		}

		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			resp.Rejected++
			resp.Errors = append(resp.Errors, IngestError{
				Index: index,
				Error: fmt.Sprintf("failed to parse event structure: %v", err),
			})
			continue
		}

		// Field validations
		if ev.EventID == "" {
			resp.Rejected++
			resp.Errors = append(resp.Errors, IngestError{
				Index:   index,
				EventID: ev.EventID,
				Error:   "missing required field: event_id",
			})
			continue
		}

		if ev.CampaignID == "" {
			resp.Rejected++
			resp.Errors = append(resp.Errors, IngestError{
				Index:   index,
				EventID: ev.EventID,
				Error:   "missing required field: campaign_id",
			})
			continue
		}

		if ev.ContactID == "" {
			resp.Rejected++
			resp.Errors = append(resp.Errors, IngestError{
				Index:   index,
				EventID: ev.EventID,
				Error:   "missing required field: contact_id",
			})
			continue
		}

		if ev.Type != "sent" && ev.Type != "delivered" && ev.Type != "opened" && ev.Type != "clicked" {
			resp.Rejected++
			resp.Errors = append(resp.Errors, IngestError{
				Index:   index,
				EventID: ev.EventID,
				Error:   fmt.Sprintf("invalid event type: %s", ev.Type),
			})
			continue
		}

		parsedTime, err := parseTimestamp(ev.Timestamp)
		if err != nil {
			resp.Rejected++
			resp.Errors = append(resp.Errors, IngestError{
				Index:   index,
				EventID: ev.EventID,
				Error:   err.Error(),
			})
			continue
		}
		ev.ParsedTimestamp = parsedTime

		// Thread-safe state update
		s.mu.Lock()
		if s.seenEvents[ev.EventID] {
			s.mu.Unlock()
			resp.Duplicates++
			continue
		}

		s.seenEvents[ev.EventID] = true

		cd, ok := s.campaigns[ev.CampaignID]
		if !ok {
			cd = &CampaignData{
				ID:           ev.CampaignID,
				UniqueOpens:  make(map[string]bool),
				UniqueClicks: make(map[string]bool),
				DailyBuckets: make(map[string]*DailyBucket),
				EventLog:     make([]Event, 0),
			}
			s.campaigns[ev.CampaignID] = cd
		}

		dateKey := ev.ParsedTimestamp.Format("2006-01-02")
		bucket, ok := cd.DailyBuckets[dateKey]
		if !ok {
			bucket = &DailyBucket{Date: dateKey}
			cd.DailyBuckets[dateKey] = bucket
		}

		switch ev.Type {
		case "sent":
			cd.Sent++
			bucket.Sent++
		case "delivered":
			cd.Delivered++
			bucket.Delivered++
		case "opened":
			cd.Opened++
			bucket.Opened++
			cd.UniqueOpens[ev.ContactID] = true
		case "clicked":
			cd.Clicked++
			bucket.Clicked++
			cd.UniqueClicks[ev.ContactID] = true
		}

		cd.EventLog = append(cd.EventLog, ev)
		s.mu.Unlock()

		resp.Processed++
	}

	// Consume closing bracket ']'
	_, _ = dec.Token()

	return resp, nil
}

// IngestBytes processes raw bytes as a JSON batch.
func (s *Store) IngestBytes(data []byte) (*IngestResponse, error) {
	return s.IngestBatch(bytes.NewReader(data))
}

// GetCampaignStats retrieves campaign statistics. Returns false if campaign not found.
func (s *Store) GetCampaignStats(campaignID string) (*CampaignStatsResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cd, ok := s.campaigns[campaignID]
	if !ok {
		return nil, false
	}

	uniqueOpens := int64(len(cd.UniqueOpens))
	uniqueClicks := int64(len(cd.UniqueClicks))

	var deliveryRate, openRate, clickRate float64
	if cd.Sent > 0 {
		deliveryRate = roundFloat(float64(cd.Delivered)/float64(cd.Sent), 4)
	}
	if cd.Delivered > 0 {
		openRate = roundFloat(float64(uniqueOpens)/float64(cd.Delivered), 4)
	}
	if cd.Opened > 0 {
		clickRate = roundFloat(float64(uniqueClicks)/float64(cd.Opened), 4)
	}

	dailyStats := make([]DailyBucket, 0, len(cd.DailyBuckets))
	for _, b := range cd.DailyBuckets {
		dailyStats = append(dailyStats, *b)
	}
	sort.Slice(dailyStats, func(i, j int) bool {
		return dailyStats[i].Date < dailyStats[j].Date
	})

	return &CampaignStatsResponse{
		CampaignID:   cd.ID,
		Sent:         cd.Sent,
		Delivered:    cd.Delivered,
		Opened:       cd.Opened,
		Clicked:      cd.Clicked,
		UniqueOpens:  uniqueOpens,
		UniqueClicks: uniqueClicks,
		DeliveryRate: deliveryRate,
		OpenRate:     openRate,
		ClickRate:    clickRate,
		DailyStats:   dailyStats,
	}, true
}

// GetCampaignEvents returns paginated event logs for a campaign.
func (s *Store) GetCampaignEvents(campaignID string, limit, offset int, eventType string) (*PaginatedEventsResponse, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cd, ok := s.campaigns[campaignID]
	if !ok {
		return nil, false
	}

	filtered := make([]Event, 0, len(cd.EventLog))
	for _, ev := range cd.EventLog {
		if eventType == "" || ev.Type == eventType {
			filtered = append(filtered, ev)
		}
	}

	total := len(filtered)
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	paged := make([]Event, 0)
	if offset < total {
		end := offset + limit
		if end > total {
			end = total
		}
		paged = filtered[offset:end]
	}

	return &PaginatedEventsResponse{
		CampaignID: campaignID,
		Total:      total,
		Limit:      limit,
		Offset:     offset,
		Events:     paged,
	}, true
}

func roundFloat(val float64, precision int) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}
