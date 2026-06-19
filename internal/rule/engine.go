package rule

import (
	"regexp"
	"strings"
	"sync/atomic"

	"logmonitor/internal/config"
)

type AlertConfigSnapshot struct {
	Threshold       int
	WindowSeconds   int
	CooldownSeconds int
}

type compiledRule struct {
	ruleID       string
	ruleName     string
	matchType    config.MatchType
	patternLower string
	regex        *regexp.Regexp
	tags         map[string]string
	alert        AlertConfigSnapshot
}

type ruleSnapshot struct {
	rules []compiledRule
}

type Engine struct {
	snapshot atomic.Pointer[ruleSnapshot]
}

type MatchResult struct {
	RuleID   string
	RuleName string
	Tags     map[string]string
	Alert    AlertConfigSnapshot
}

func buildSnapshot(cfg *config.AppConfig) *ruleSnapshot {
	rules := make([]compiledRule, 0, len(cfg.Rules))
	for _, r := range cfg.Rules {
		if !r.Enabled {
			continue
		}
		cr := compiledRule{
			ruleID:       r.ID,
			ruleName:     r.Name,
			matchType:    r.Type,
			patternLower: strings.ToLower(r.Pattern),
		}
		if r.Type == config.MatchRegex {
			re, err := regexp.Compile(r.Pattern)
			if err != nil {
				continue
			}
			cr.regex = re
		}
		if r.Tags != nil {
			cr.tags = make(map[string]string, len(r.Tags))
			for k, v := range r.Tags {
				cr.tags[k] = v
			}
		}
		ac := r.AlertConfig
		if ac == nil {
			ac = config.DefaultAlertConfig()
		}
		threshold := ac.Threshold
		if threshold <= 0 {
			threshold = 5
		}
		window := ac.WindowSeconds
		if window <= 0 {
			window = 60
		}
		cooldown := ac.CooldownSeconds
		if cooldown <= 0 {
			cooldown = 30
		}
		cr.alert = AlertConfigSnapshot{
			Threshold:       threshold,
			WindowSeconds:   window,
			CooldownSeconds: cooldown,
		}
		rules = append(rules, cr)
	}
	return &ruleSnapshot{rules: rules}
}

func NewEngine(cfg *config.AppConfig) *Engine {
	e := &Engine{}
	e.snapshot.Store(buildSnapshot(cfg))
	return e
}

func (e *Engine) Reload(cfg *config.AppConfig) {
	e.snapshot.Store(buildSnapshot(cfg))
}

func (e *Engine) Match(content string) []MatchResult {
	snap := e.snapshot.Load()
	rules := snap.rules

	var results []MatchResult
	lowerContent := strings.ToLower(content)

	for _, cr := range rules {
		matched := false
		switch cr.matchType {
		case config.MatchKeyword, config.MatchContains:
			if strings.Contains(lowerContent, cr.patternLower) {
				matched = true
			}
		case config.MatchRegex:
			if cr.regex != nil && cr.regex.MatchString(content) {
				matched = true
			}
		}
		if matched {
			tagsCopy := make(map[string]string, len(cr.tags))
			for k, v := range cr.tags {
				tagsCopy[k] = v
			}
			results = append(results, MatchResult{
				RuleID:   cr.ruleID,
				RuleName: cr.ruleName,
				Tags:     tagsCopy,
				Alert:    cr.alert,
			})
		}
	}
	return results
}
