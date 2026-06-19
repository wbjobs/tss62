package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"logmonitor/internal/alert"
	redisstore "logmonitor/internal/redis"
	"logmonitor/internal/rule"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  16384,
	WriteBufferSize: 16384,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type LogMessage struct {
	Timestamp int64  `json:"timestamp"`
	Source    string `json:"source"`
	Content   string `json:"content"`
}

type Server struct {
	engine      *rule.Engine
	store       *redisstore.Store
	alertMgr    *alert.Manager
	connCount   int64
	workerPool  chan struct{}
	wg          sync.WaitGroup
}

func NewServer(engine *rule.Engine, store *redisstore.Store, alertMgr *alert.Manager, workerSize int) *Server {
	if workerSize <= 0 {
		workerSize = 64
	}
	return &Server{
		engine:     engine,
		store:      store,
		alertMgr:   alertMgr,
		workerPool: make(chan struct{}, workerSize),
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	connID := atomic.AddInt64(&s.connCount, 1)
	source := r.URL.Query().Get("source")
	if source == "" {
		source = fmt.Sprintf("client-%d", connID)
	}

	conn.SetReadLimit(10 * 1024 * 1024)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("ws read error from %s: %v", source, err)
			}
			break
		}
		s.wg.Add(1)
		select {
		case s.workerPool <- struct{}{}:
			go s.processMessage(msg, source)
		default:
			go s.processMessage(msg, source)
		}
	}
	s.wg.Wait()
}

func (s *Server) processMessage(msg []byte, source string) {
	defer s.wg.Done()
	defer func() {
		if len(s.workerPool) > 0 {
			<-s.workerPool
		}
	}()

	var lm LogMessage
	if err := json.Unmarshal(msg, &lm); err != nil {
		lm = LogMessage{
			Timestamp: time.Now().UnixMilli(),
			Source:    source,
			Content:   string(msg),
		}
	}
	if lm.Source == "" {
		lm.Source = source
	}
	if lm.Timestamp == 0 {
		lm.Timestamp = time.Now().UnixMilli()
	}

	matches := s.engine.Match(lm.Content)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, m := range matches {
		entry := &redisstore.MatchedLogEntry{
			RuleID:    m.RuleID,
			RuleName:  m.RuleName,
			Timestamp: lm.Timestamp,
			Content:   lm.Content,
			Source:    lm.Source,
			Tags:      m.Tags,
		}
		if err := s.store.SaveMatchedLog(ctx, entry); err != nil {
			log.Printf("save matched log failed: %v", err)
		}
		s.alertMgr.Record(m.RuleID, m.RuleName, lm.Content, m.Tags)
	}
}

func (s *Server) Start(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("collector server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
