package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type MatchedLogEntry struct {
	RuleID    string            `json:"rule_id"`
	RuleName  string            `json:"rule_name"`
	Timestamp int64             `json:"timestamp"`
	Content   string            `json:"content"`
	Source    string            `json:"source"`
	Tags      map[string]string `json:"tags,omitempty"`
}

type Store struct {
	client *redis.Client
	prefix string
}

func NewStore(addr, password string, db int) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     100,
		MinIdleConns: 20,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return &Store{
		client: client,
		prefix: "logmonitor:",
	}, nil
}

func (s *Store) keyForRule(ruleID string) string {
	return s.prefix + "matched:" + ruleID
}

func (s *Store) SaveMatchedLog(ctx context.Context, entry *MatchedLogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}
	z := redis.Z{
		Score:  float64(entry.Timestamp),
		Member: data,
	}
	key := s.keyForRule(entry.RuleID)
	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, key, z)
	pipe.ZRemRangeByRank(ctx, key, 0, -10001)
	pipe.Expire(ctx, key, 7*24*time.Hour)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis zadd pipeline: %w", err)
	}
	return nil
}

func (s *Store) GetMatchedLogs(ctx context.Context, ruleID string, limit int64) ([]*MatchedLogEntry, error) {
	key := s.keyForRule(ruleID)
	results, err := s.client.ZRevRange(ctx, key, 0, limit-1).Result()
	if err != nil {
		return nil, fmt.Errorf("redis zrevrange: %w", err)
	}
	entries := make([]*MatchedLogEntry, 0, len(results))
	for _, r := range results {
		var entry MatchedLogEntry
		if err := json.Unmarshal([]byte(r), &entry); err != nil {
			continue
		}
		entries = append(entries, &entry)
	}
	return entries, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}
