package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"logmonitor/internal/alert"
	"logmonitor/internal/clientmgr"
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

type task struct {
	msg      []byte
	source   string
	clientID int64
}

type Server struct {
	engine      *rule.Engine
	store       *redisstore.Store
	alertMgr    *alert.Manager
	clientMgr   *clientmgr.Manager
	connCount   int64
	dropped     uint64
	processed   uint64
	taskQueue   chan *task
	workerCount int
}

func NewServer(engine *rule.Engine, store *redisstore.Store, alertMgr *alert.Manager, clientMgr *clientmgr.Manager, workerCount, queueSize int) *Server {
	if workerCount <= 0 {
		workerCount = 64
	}
	if queueSize <= 0 {
		queueSize = 65536
	}
	s := &Server{
		engine:      engine,
		store:       store,
		alertMgr:    alertMgr,
		clientMgr:   clientMgr,
		taskQueue:   make(chan *task, queueSize),
		workerCount: workerCount,
	}
	for i := 0; i < workerCount; i++ {
		go s.worker()
	}
	return s
}

func (s *Server) worker() {
	for t := range s.taskQueue {
		s.processMessage(t.msg, t.source, t.clientID)
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

	var clientID int64
	if s.clientMgr != nil {
		clientID = s.clientMgr.Register(source)
		defer s.clientMgr.Unregister(clientID)
	}

	conn.SetReadLimit(10 * 1024 * 1024)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
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
		t := &task{msg: msg, source: source, clientID: clientID}
		select {
		case s.taskQueue <- t:
			atomic.AddUint64(&s.processed, 1)
		default:
			atomic.AddUint64(&s.dropped, 1)
			dropped := atomic.LoadUint64(&s.dropped)
			if dropped%1000 == 0 {
				log.Printf("task queue full, dropped %d messages total", dropped)
			}
		}
	}
}

func (s *Server) processMessage(msg []byte, source string, clientID int64) {
	if s.clientMgr != nil && clientID != 0 {
		s.clientMgr.RecordMessage(clientID)
	}

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
		s.alertMgr.Record(m, lm.Content)
	}
}

func (s *Server) Start(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ok connections=%d processed=%d dropped=%d",
			atomic.LoadInt64(&s.connCount),
			atomic.LoadUint64(&s.processed),
			atomic.LoadUint64(&s.dropped),
		)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("collector server listening on %s (workers=%d queue=%d)", addr, s.workerCount, cap(s.taskQueue))
	return http.ListenAndServe(addr, mux)
}
