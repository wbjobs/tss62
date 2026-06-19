package alert

import (
	"sync"
	"time"
)

type AlertEvent struct {
	RuleID      string            `json:"rule_id"`
	RuleName    string            `json:"rule_name"`
	Count       int               `json:"count"`
	WindowStart int64             `json:"window_start"`
	WindowEnd   int64             `json:"window_end"`
	Timestamp   int64             `json:"timestamp"`
	Tags        map[string]string `json:"tags,omitempty"`
	SampleLogs  []string          `json:"sample_logs,omitempty"`
}

type ruleState struct {
	windowSize time.Duration
	threshold  int
	cooldown   time.Duration
	timestamps []int64
	samples    []string
	lastAlert  int64
}

type Manager struct {
	mu        sync.RWMutex
	rules     map[string]*ruleState
	listeners map[chan *AlertEvent]struct{}
}

func NewManager() *Manager {
	return &Manager{
		rules:     make(map[string]*ruleState),
		listeners: make(map[chan *AlertEvent]struct{}),
	}
}

func (am *Manager) RegisterRule(ruleID string, windowSeconds, threshold, cooldownSeconds int) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.rules[ruleID] = &ruleState{
		windowSize: time.Duration(windowSeconds) * time.Second,
		threshold:  threshold,
		cooldown:   time.Duration(cooldownSeconds) * time.Second,
		timestamps: make([]int64, 0),
		samples:    make([]string, 0),
	}
}

func (am *Manager) UnregisterRule(ruleID string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	delete(am.rules, ruleID)
}

func (am *Manager) ReloadRules(rules map[string][3]int) {
	am.mu.Lock()
	defer am.mu.Unlock()
	newRules := make(map[string]*ruleState, len(rules))
	for id, cfg := range rules {
		if existing, ok := am.rules[id]; ok {
			existing.windowSize = time.Duration(cfg[0]) * time.Second
			existing.threshold = cfg[1]
			existing.cooldown = time.Duration(cfg[2]) * time.Second
			newRules[id] = existing
		} else {
			newRules[id] = &ruleState{
				windowSize: time.Duration(cfg[0]) * time.Second,
				threshold:  cfg[1],
				cooldown:   time.Duration(cfg[2]) * time.Second,
				timestamps: make([]int64, 0),
				samples:    make([]string, 0),
			}
		}
	}
	am.rules = newRules
}

func (am *Manager) Record(ruleID, ruleName string, logContent string, tags map[string]string) {
	now := time.Now().UnixMilli()

	am.mu.Lock()
	state, ok := am.rules[ruleID]
	if !ok {
		am.mu.Unlock()
		return
	}

	cutoff := now - state.windowSize.Milliseconds()
	i := 0
	for i < len(state.timestamps) && state.timestamps[i] < cutoff {
		i++
	}
	if i > 0 {
		state.timestamps = state.timestamps[i:]
		if i <= len(state.samples) {
			state.samples = state.samples[i:]
		} else {
			state.samples = state.samples[:0]
		}
	}

	state.timestamps = append(state.timestamps, now)
	if len(state.samples) < 5 {
		state.samples = append(state.samples, logContent)
	}

	count := len(state.timestamps)
	shouldAlert := count >= state.threshold && (now-state.lastAlert) > state.cooldown.Milliseconds()

	var event *AlertEvent
	if shouldAlert {
		state.lastAlert = now
		samplesCopy := make([]string, len(state.samples))
		copy(samplesCopy, state.samples)
		tagsCopy := make(map[string]string, len(tags))
		for k, v := range tags {
			tagsCopy[k] = v
		}
		event = &AlertEvent{
			RuleID:      ruleID,
			RuleName:    ruleName,
			Count:       count,
			WindowStart: state.timestamps[0],
			WindowEnd:   now,
			Timestamp:   now,
			Tags:        tagsCopy,
			SampleLogs:  samplesCopy,
		}
		state.timestamps = state.timestamps[:0]
		state.samples = state.samples[:0]
	}

	listeners := make([]chan *AlertEvent, 0, len(am.listeners))
	for ch := range am.listeners {
		listeners = append(listeners, ch)
	}
	am.mu.Unlock()

	if event != nil {
		for _, ch := range listeners {
			select {
			case ch <- event:
			default:
			}
		}
	}
}

func (am *Manager) Subscribe() chan *AlertEvent {
	ch := make(chan *AlertEvent, 100)
	am.mu.Lock()
	am.listeners[ch] = struct{}{}
	am.mu.Unlock()
	return ch
}

func (am *Manager) Unsubscribe(ch chan *AlertEvent) {
	am.mu.Lock()
	delete(am.listeners, ch)
	am.mu.Unlock()
	close(ch)
}
