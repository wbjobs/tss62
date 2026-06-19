package clientmgr

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type ClientStats struct {
	ClientID    int64   `json:"client_id"`
	Source      string  `json:"source"`
	ConnectedAt int64   `json:"connected_at"`
	LastActive  int64   `json:"last_active"`
	TotalMsgs   uint64  `json:"total_msgs"`
	Rate1s      float64 `json:"rate_1s"`
	Rate5s      float64 `json:"rate_5s"`
	Rate1m      float64 `json:"rate_1m"`
}

type clientState struct {
	clientID   int64
	source     string
	connected  int64
	lastActive int64
	totalMsgs  uint64
	window1s   []int64
	window5s   []int64
	window1m   []int64
}

type Manager struct {
	mu      sync.RWMutex
	clients map[int64]*clientState
	counter int64
}

func NewManager() *Manager {
	m := &Manager{
		clients: make(map[int64]*clientState),
	}
	go m.cleanupLoop()
	return m
}

func (m *Manager) Register(source string) int64 {
	id := atomic.AddInt64(&m.counter, 1)
	now := time.Now().UnixMilli()
	m.mu.Lock()
	m.clients[id] = &clientState{
		clientID:   id,
		source:     source,
		connected:  now,
		lastActive: now,
		window1s:   make([]int64, 0),
		window5s:   make([]int64, 0),
		window1m:   make([]int64, 0),
	}
	m.mu.Unlock()
	return id
}

func (m *Manager) Unregister(id int64) {
	m.mu.Lock()
	delete(m.clients, id)
	m.mu.Unlock()
}

func (m *Manager) RecordMessage(id int64) {
	now := time.Now().UnixMilli()
	m.mu.Lock()
	cs, ok := m.clients[id]
	if ok {
		cs.totalMsgs++
		cs.lastActive = now
		cs.window1s = append(cs.window1s, now)
		cs.window5s = append(cs.window5s, now)
		cs.window1m = append(cs.window1m, now)
	}
	m.mu.Unlock()
}

func (m *Manager) pruneWindow(ts []int64, cutoff int64) []int64 {
	i := 0
	for i < len(ts) && ts[i] < cutoff {
		i++
	}
	if i == 0 {
		return ts
	}
	return ts[i:]
}

func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now().UnixMilli()
		m.mu.Lock()
		for _, cs := range m.clients {
			cs.window1s = m.pruneWindow(cs.window1s, now-1000)
			cs.window5s = m.pruneWindow(cs.window5s, now-5000)
			cs.window1m = m.pruneWindow(cs.window1m, now-60000)
		}
		m.mu.Unlock()
	}
}

func (m *Manager) GetAllStats() []ClientStats {
	now := time.Now().UnixMilli()
	m.mu.RLock()
	defer m.mu.RUnlock()
	stats := make([]ClientStats, 0, len(m.clients))
	for _, cs := range m.clients {
		rate1s := float64(len(cs.window1s))
		rate5s := float64(len(cs.window5s)) / 5.0
		rate1m := float64(len(cs.window1m)) / 60.0
		stats = append(stats, ClientStats{
			ClientID:    cs.clientID,
			Source:      cs.source,
			ConnectedAt: cs.connected,
			LastActive:  cs.lastActive,
			TotalMsgs:   cs.totalMsgs,
			Rate1s:      rate1s,
			Rate5s:      rate5s,
			Rate1m:      rate1m,
		})
	}
	_ = now
	return stats
}

func (m *Manager) GetStats(id int64) (ClientStats, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cs, ok := m.clients[id]
	if !ok {
		return ClientStats{}, false
	}
	return ClientStats{
		ClientID:    cs.clientID,
		Source:      cs.source,
		ConnectedAt: cs.connected,
		LastActive:  cs.lastActive,
		TotalMsgs:   cs.totalMsgs,
		Rate1s:      float64(len(cs.window1s)),
		Rate5s:      float64(len(cs.window5s)) / 5.0,
		Rate1m:      float64(len(cs.window1m)) / 60.0,
	}, true
}

func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/clients", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := m.GetAllStats()
		resp := map[string]interface{}{
			"total":   len(stats),
			"clients": stats,
		}
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/api/clients/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var id int64
		_, err := fmt.Sscanf(r.URL.Path, "/api/clients/%d", &id)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		stats, ok := m.GetStats(id)
		if !ok {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(stats)
	})
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := m.GetAllStats()
		var totalRate float64
		for _, s := range stats {
			totalRate += s.Rate1s
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"connected_clients": len(stats),
			"total_rate_1s":     totalRate,
		})
	})
	return mux
}

func (m *Manager) Start(port int) error {
	mux := m.Handler()
	addr := fmt.Sprintf(":%d", port)
	return http.ListenAndServe(addr, mux)
}
