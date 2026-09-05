package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

var globalStore = NewStore()

func main() {
	mux := setupRouter(globalStore)

	addr := ":8080"
	fmt.Printf("listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func setupRouter(store *Store) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", makePostEventsHandler(store))
	mux.HandleFunc("GET /campaigns/{campaignID}/stats", makeGetStatsHandler(store))
	mux.HandleFunc("GET /campaigns/{campaignID}/events", makeGetEventsHandler(store))
	return mux
}

// makePostEventsHandler ingests a JSON array of events.
func makePostEventsHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}

		resp, err := store.IngestBatch(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// makeGetStatsHandler returns aggregated stats for one campaign.
func makeGetStatsHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		campaignID := getCampaignID(r)
		if campaignID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing campaign_id in path"})
			return
		}

		stats, found := store.GetCampaignStats(campaignID)
		if !found {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "campaign not found"})
			return
		}

		writeJSON(w, http.StatusOK, stats)
	}
}

// makeGetEventsHandler returns a paginated list of events for one campaign.
func makeGetEventsHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		campaignID := getCampaignID(r)
		if campaignID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing campaign_id in path"})
			return
		}

		query := r.URL.Query()
		limit, _ := strconv.Atoi(query.Get("limit"))
		offset, _ := strconv.Atoi(query.Get("offset"))
		eventType := query.Get("type")

		events, found := store.GetCampaignEvents(campaignID, limit, offset, eventType)
		if !found {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "campaign not found"})
			return
		}

		writeJSON(w, http.StatusOK, events)
	}
}

func getCampaignID(r *http.Request) string {
	id := r.PathValue("campaignID")
	if id != "" {
		return id
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "campaigns" {
		return parts[1]
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON: %v", err)
	}
}
