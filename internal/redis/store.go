package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	client       *redis.Client
	prefix       string
	retention    time.Duration
	maxEntries   int64
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
	s := &Store{
		client:     client,
		prefix:     "logmonitor:",
		retention:  24 * time.Hour,
		maxEntries: 10000,
	}
	go s.cleanupLoop()
	return s, nil
}

func (s *Store) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		s.cleanupExpired(ctx)
		cancel()
	}
}

func (s *Store) cleanupExpired(ctx context.Context) {
	pattern := s.prefix + "matched:*"
	iter := s.client.Scan(ctx, 0, pattern, 100).Iterator()
	cutoff := float64(time.Now().Add(-s.retention).UnixMilli())
	for iter.Next(ctx) {
		key := iter.Val()
		pipe := s.client.Pipeline()
		pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%f", cutoff))
		pipe.ZRemRangeByRank(ctx, key, 0, -s.maxEntries-1)
		pipe.Expire(ctx, key, s.retention*2)
		_, err := pipe.Exec(ctx)
		if err != nil {
			log.Printf("cleanup %s failed: %v", key, err)
		}
	}
	if err := iter.Err(); err != nil {
		log.Printf("scan keys failed: %v", err)
	}
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
	cutoff := float64(time.Now().Add(-s.retention).UnixMilli())
	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, key, z)
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%f", cutoff))
	pipe.ZRemRangeByRank(ctx, key, 0, -s.maxEntries-1)
	pipe.Expire(ctx, key, s.retention*2)
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
