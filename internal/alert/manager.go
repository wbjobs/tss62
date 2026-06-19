package alert

import (
	"sync"
	"time"

	"logmonitor/internal/rule"
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
	timestamps []int64
	samples    []string
	lastAlert  int64
}

type Manager struct {
	mu         sync.RWMutex
	rules      map[string]*ruleState
	listeners  map[chan *AlertEvent]struct{}
	notifier   Notifier
}

type Notifier interface {
	Send(evt *AlertEvent)
}

func NewManager() *Manager {
	return &Manager{
		rules:     make(map[string]*ruleState),
		listeners: make(map[chan *AlertEvent]struct{}),
	}
}

func (am *Manager) SetNotifier(n Notifier) {
	am.mu.Lock()
	am.notifier = n
	am.mu.Unlock()
}

func (am *Manager) Record(result rule.MatchResult, logContent string) {
	now := time.Now().UnixMilli()
	windowSize := time.Duration(result.Alert.WindowSeconds) * time.Second
	cooldown := time.Duration(result.Alert.CooldownSeconds) * time.Second
	threshold := result.Alert.Threshold

	am.mu.Lock()
	state, ok := am.rules[result.RuleID]
	if !ok {
		state = &ruleState{
			timestamps: make([]int64, 0),
			samples:    make([]string, 0),
		}
		am.rules[result.RuleID] = state
	}

	cutoff := now - windowSize.Milliseconds()
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
	shouldAlert := count >= threshold && (now-state.lastAlert) > cooldown.Milliseconds()

	var event *AlertEvent
	if shouldAlert {
		state.lastAlert = now
		samplesCopy := make([]string, len(state.samples))
		copy(samplesCopy, state.samples)
		tagsCopy := make(map[string]string, len(result.Tags))
		for k, v := range result.Tags {
			tagsCopy[k] = v
		}
		event = &AlertEvent{
			RuleID:      result.RuleID,
			RuleName:    result.RuleName,
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
	var notifier Notifier
	if am.notifier != nil {
		notifier = am.notifier
	}
	am.mu.Unlock()

	if event != nil {
		if notifier != nil {
			notifier.Send(event)
		}
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
