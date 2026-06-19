package rule

import (
	"regexp"
	"strings"
	"sync"

	"logmonitor/internal/config"
)

type compiledRule struct {
	rule   config.Rule
	regex  *regexp.Regexp
	lower  string
}

type Engine struct {
	mu      sync.RWMutex
	rules   []compiledRule
	enabled map[string]bool
}

type MatchResult struct {
	RuleID   string
	RuleName string
	Tags     map[string]string
}

func NewEngine(cfg *config.AppConfig) *Engine {
	e := &Engine{
		enabled: make(map[string]bool),
	}
	e.Reload(cfg)
	return e
}

func (e *Engine) Reload(cfg *config.AppConfig) {
	compiled := make([]compiledRule, 0, len(cfg.Rules))
	enabled := make(map[string]bool, len(cfg.Rules))
	for _, r := range cfg.Rules {
		if !r.Enabled {
			continue
		}
		cr := compiledRule{
			rule:  r,
			lower: strings.ToLower(r.Pattern),
		}
		if r.Type == config.MatchRegex {
			re, err := regexp.Compile(r.Pattern)
			if err != nil {
				continue
			}
			cr.regex = re
		}
		compiled = append(compiled, cr)
		enabled[r.ID] = true
	}
	e.mu.Lock()
	e.rules = compiled
	e.enabled = enabled
	e.mu.Unlock()
}

func (e *Engine) Match(content string) []MatchResult {
	e.mu.RLock()
	rules := e.rules
	e.mu.RUnlock()

	var results []MatchResult
	lowerContent := strings.ToLower(content)

	for _, cr := range rules {
		matched := false
		switch cr.rule.Type {
		case config.MatchKeyword, config.MatchContains:
			if strings.Contains(lowerContent, cr.lower) {
				matched = true
			}
		case config.MatchRegex:
			if cr.regex != nil && cr.regex.MatchString(content) {
				matched = true
			}
		}
		if matched {
			results = append(results, MatchResult{
				RuleID:   cr.rule.ID,
				RuleName: cr.rule.Name,
				Tags:     cr.rule.Tags,
			})
		}
	}
	return results
}

func (e *Engine) GetAlertConfig(ruleID string) *config.AlertRuleConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, cr := range e.rules {
		if cr.rule.ID == ruleID {
			return cr.rule.AlertConfig
		}
	}
	return nil
}
