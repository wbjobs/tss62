package config

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

type MatchType string

const (
	MatchKeyword   MatchType = "keyword"
	MatchRegex     MatchType = "regex"
	MatchContains  MatchType = "contains"
)

type Rule struct {
	ID          string            `yaml:"id"`
	Name        string            `yaml:"name"`
	Type        MatchType         `yaml:"type"`
	Pattern     string            `yaml:"pattern"`
	Enabled     bool              `yaml:"enabled"`
	Tags        map[string]string `yaml:"tags,omitempty"`
	AlertConfig *AlertRuleConfig  `yaml:"alert,omitempty"`
}

type AlertRuleConfig struct {
	Threshold   int    `yaml:"threshold"`
	WindowSeconds int  `yaml:"window_seconds"`
	CooldownSeconds int `yaml:"cooldown_seconds,omitempty"`
}

type AppConfig struct {
	CollectorPort int             `yaml:"collector_port"`
	AdminPort     int             `yaml:"admin_port"`
	ManagerPort   int             `yaml:"manager_port"`
	RedisAddr     string          `yaml:"redis_addr"`
	RedisPassword string          `yaml:"redis_password,omitempty"`
	RedisDB       int             `yaml:"redis_db"`
	Rules         []Rule          `yaml:"rules"`
	DingTalk      []DingTalkCfg   `yaml:"dingtalk,omitempty"`
	WeChat        []WeChatCfg     `yaml:"wechat,omitempty"`
}

type DingTalkCfg struct {
	WebhookURL string `yaml:"webhook_url"`
	Secret     string `yaml:"secret,omitempty"`
}

type WeChatCfg struct {
	WebhookURL string `yaml:"webhook_url"`
}

type ConfigLoader struct {
	path         string
	config       *AppConfig
	mu           sync.RWMutex
	watcher      *fsnotify.Watcher
	onChange     func(*AppConfig)
	debounceDur  time.Duration
	debounceMu   sync.Mutex
	debounceTimer *time.Timer
}

func DefaultAlertConfig() *AlertRuleConfig {
	return &AlertRuleConfig{
		Threshold:       5,
		WindowSeconds:   60,
		CooldownSeconds: 30,
	}
}

func DefaultConfig() *AppConfig {
	return &AppConfig{
		CollectorPort: 8080,
		AdminPort:     8081,
		ManagerPort:   8082,
		RedisAddr:     "localhost:6379",
		RedisDB:       0,
	}
}

func Load(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config yaml: %w", err)
	}
	for i := range cfg.Rules {
		if cfg.Rules[i].AlertConfig == nil {
			cfg.Rules[i].AlertConfig = DefaultAlertConfig()
		} else {
			if cfg.Rules[i].AlertConfig.Threshold <= 0 {
				cfg.Rules[i].AlertConfig.Threshold = 5
			}
			if cfg.Rules[i].AlertConfig.WindowSeconds <= 0 {
				cfg.Rules[i].AlertConfig.WindowSeconds = 60
			}
		}
	}
	return cfg, nil
}

func NewConfigLoader(path string, onChange func(*AppConfig)) (*ConfigLoader, error) {
	cl := &ConfigLoader{
		path:        path,
		onChange:    onChange,
		debounceDur: 500 * time.Millisecond,
	}
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	cl.config = cfg

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create watcher: %w", err)
	}
	cl.watcher = watcher

	if err := watcher.Add(path); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("watch config file: %w", err)
	}

	go cl.watchLoop()

	return cl, nil
}

func (cl *ConfigLoader) watchLoop() {
	for {
		select {
		case event, ok := <-cl.watcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) != 0 {
				cl.debounceMu.Lock()
				if cl.debounceTimer != nil {
					cl.debounceTimer.Stop()
				}
				cl.debounceTimer = time.AfterFunc(cl.debounceDur, func() {
					cfg, err := Load(cl.path)
					if err != nil {
						fmt.Fprintf(os.Stderr, "reload config failed: %v\n", err)
						return
					}
					cl.mu.Lock()
					cl.config = cfg
					cl.mu.Unlock()
					if cl.onChange != nil {
						cl.onChange(cfg)
					}
				})
				cl.debounceMu.Unlock()
			}
		case err, ok := <-cl.watcher.Errors:
			if !ok {
				return
			}
			fmt.Fprintf(os.Stderr, "config watch error: %v\n", err)
		}
	}
}

func (cl *ConfigLoader) Get() *AppConfig {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.config
}

func (cl *ConfigLoader) Close() error {
	if cl.watcher != nil {
		return cl.watcher.Close()
	}
	return nil
}
