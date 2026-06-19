package notify

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"logmonitor/internal/alert"
)

type Notifier interface {
	Send(evt *alert.AlertEvent) error
	Name() string
}

type DingTalkConfig struct {
	WebhookURL string `yaml:"webhook_url"`
	Secret     string `yaml:"secret,omitempty"`
}

type WeChatConfig struct {
	WebhookURL string `yaml:"webhook_url"`
}

type Manager struct {
	notifiers []Notifier
	client    *http.Client
	queue     chan *alert.AlertEvent
}

func NewManager(dingTalks []DingTalkConfig, wechats []WeChatConfig) *Manager {
	m := &Manager{
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		queue: make(chan *alert.AlertEvent, 1000),
	}
	for _, cfg := range dingTalks {
		if cfg.WebhookURL != "" {
			m.notifiers = append(m.notifiers, NewDingTalkNotifier(cfg, m.client))
		}
	}
	for _, cfg := range wechats {
		if cfg.WebhookURL != "" {
			m.notifiers = append(m.notifiers, NewWeChatNotifier(cfg, m.client))
		}
	}
	go m.dispatchLoop()
	return m
}

func (m *Manager) dispatchLoop() {
	for evt := range m.queue {
		for _, n := range m.notifiers {
			go func(n Notifier, evt *alert.AlertEvent) {
				if err := n.Send(evt); err != nil {
					log.Printf("notify %s failed: %v", n.Name(), err)
				}
			}(n, evt)
		}
	}
}

func (m *Manager) Send(evt *alert.AlertEvent) {
	select {
	case m.queue <- evt:
	default:
		log.Printf("notify queue full, dropping alert %s", evt.RuleID)
	}
}

func (m *Manager) Close() {
	close(m.queue)
}

// ==================== DingTalk ====================

type dingTalkNotifier struct {
	cfg    DingTalkConfig
	client *http.Client
}

func NewDingTalkNotifier(cfg DingTalkConfig, client *http.Client) *dingTalkNotifier {
	return &dingTalkNotifier{cfg: cfg, client: client}
}

func (d *dingTalkNotifier) Name() string { return "dingtalk" }

func dingTalkSign(secret string, timestamp int64) string {
	stringToSign := fmt.Sprintf("%d\n%s", timestamp, secret)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(stringToSign))
	signData := h.Sum(nil)
	return base64.StdEncoding.EncodeToString(signData)
}

func (d *dingTalkNotifier) Send(evt *alert.AlertEvent) error {
	webhookURL := d.cfg.WebhookURL
	if d.cfg.Secret != "" {
		timestamp := time.Now().UnixMilli()
		sign := dingTalkSign(d.cfg.Secret, timestamp)
		u, err := url.Parse(webhookURL)
		if err != nil {
			return fmt.Errorf("parse webhook url: %w", err)
		}
		q := u.Query()
		q.Set("timestamp", strconv.FormatInt(timestamp, 10))
		q.Set("sign", sign)
		u.RawQuery = q.Encode()
		webhookURL = u.String()
	}

	var samplesText string
	for i, s := range evt.SampleLogs {
		samplesText += fmt.Sprintf("%d. %s\n", i+1, s)
	}
	windowDur := (evt.WindowEnd - evt.WindowStart) / 1000
	text := fmt.Sprintf(`**🔥 告警触发**

**规则名称**: %s
**规则ID**: %s
**告警级别**: %s
**命中次数**: %d 次（%d 秒内）
**触发时间**: %s

**样本日志**:
%s`,
		evt.RuleName,
		evt.RuleID,
		evt.Tags["severity"],
		evt.Count,
		windowDur,
		time.UnixMilli(evt.Timestamp).Format("2006-01-02 15:04:05"),
		samplesText,
	)

	msg := map[string]interface{}{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": fmt.Sprintf("告警: %s", evt.RuleName),
			"text":  text,
		},
		"at": map[string]interface{}{
			"isAtAll": false,
		},
	}

	body, _ := json.Marshal(msg)
	req, err := http.NewRequest("POST", webhookURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	if errcode, ok := result["errcode"].(float64); ok && errcode != 0 {
		return fmt.Errorf("dingtalk api error: %s", string(respBody))
	}
	return nil
}

// ==================== WeChat Work ====================

type weChatNotifier struct {
	cfg    WeChatConfig
	client *http.Client
}

func NewWeChatNotifier(cfg WeChatConfig, client *http.Client) *weChatNotifier {
	return &weChatNotifier{cfg: cfg, client: client}
}

func (w *weChatNotifier) Name() string { return "wechat" }

func (w *weChatNotifier) Send(evt *alert.AlertEvent) error {
	var samplesText string
	for i, s := range evt.SampleLogs {
		samplesText += fmt.Sprintf("> %d. %s\n", i+1, s)
	}
	windowDur := (evt.WindowEnd - evt.WindowStart) / 1000
	content := fmt.Sprintf(`<font color="warning">**🔥 告警触发**</font>

> **规则名称**: <font color="comment">%s</font>
> **规则ID**: %s
> **告警级别**: <font color="red">%s</font>
> **命中次数**: %d 次（%d 秒内）
> **触发时间**: %s

**样本日志**:
%s`,
		evt.RuleName,
		evt.RuleID,
		evt.Tags["severity"],
		evt.Count,
		windowDur,
		time.UnixMilli(evt.Timestamp).Format("2006-01-02 15:04:05"),
		samplesText,
	)

	msg := map[string]interface{}{
		"msgtype": "markdown",
		"markdown": map[string]interface{}{
			"content": content,
		},
	}

	body, _ := json.Marshal(msg)
	req, err := http.NewRequest("POST", w.cfg.WebhookURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	if errcode, ok := result["errcode"].(float64); ok && errcode != 0 {
		return fmt.Errorf("wechat api error: %s", string(respBody))
	}
	return nil
}
