package main

import "time"

// Event represents one webhook event from a message delivery provider.
type Event struct {
	EventID    string            `json:"event_id"`
	CampaignID string            `json:"campaign_id"`
	ContactID  string            `json:"contact_id"`
	Type       string            `json:"type"` // sent | delivered | opened | clicked
	Timestamp  string            `json:"timestamp"`
	Metadata   map[string]string `json:"metadata,omitempty"`

	// ParsedTimestamp holds the validated time in UTC (not serialized to JSON).
	ParsedTimestamp time.Time `json:"-"`
}

// IngestError detail for malformed batch items.
type IngestError struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Error   string `json:"error"`
}

// IngestResponse returns batch summary metrics for POST /events.
type IngestResponse struct {
	Status     string        `json:"status"`
	Received   int           `json:"received"`
	Processed  int           `json:"processed"`
	Duplicates int           `json:"duplicates"`
	Rejected   int           `json:"rejected"`
	Errors     []IngestError `json:"errors,omitempty"`
}

// DailyBucket holds daily metric aggregations for a single date (UTC).
type DailyBucket struct {
	Date      string `json:"date"`
	Sent      int64  `json:"sent"`
	Delivered int64  `json:"delivered"`
	Opened    int64  `json:"opened"`
	Clicked   int64  `json:"clicked"`
}

// CampaignStatsResponse is returned by GET /campaigns/{campaign_id}/stats.
type CampaignStatsResponse struct {
	CampaignID   string        `json:"campaign_id"`
	Sent         int64         `json:"sent"`
	Delivered    int64         `json:"delivered"`
	Opened       int64         `json:"opened"`
	Clicked      int64         `json:"clicked"`
	UniqueOpens  int64         `json:"unique_opens"`
	UniqueClicks int64         `json:"unique_clicks"`
	DeliveryRate float64       `json:"delivery_rate"`
	OpenRate     float64       `json:"open_rate"`
	ClickRate    float64       `json:"click_rate"`
	DailyStats   []DailyBucket `json:"daily_stats"`
}

// PaginatedEventsResponse is returned by GET /campaigns/{campaign_id}/events.
type PaginatedEventsResponse struct {
	CampaignID string  `json:"campaign_id"`
	Total      int     `json:"total"`
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Events     []Event `json:"events"`
}
