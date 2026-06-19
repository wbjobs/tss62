package notifier

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"logmonitor/internal/alert"
)

var adminUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 16384,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type adminClient struct {
	conn *websocket.Conn
	send chan []byte
}

type Server struct {
	alertMgr *alert.Manager
	clients  map[*adminClient]struct{}
	mu       sync.RWMutex
	alertCh  chan *alert.AlertEvent
}

func NewServer(alertMgr *alert.Manager) *Server {
	return &Server{
		alertMgr: alertMgr,
		clients:  make(map[*adminClient]struct{}),
		alertCh:  make(chan *alert.AlertEvent, 1000),
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := adminUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("admin ws upgrade failed: %v", err)
		return
	}

	client := &adminClient{
		conn: conn,
		send: make(chan []byte, 256),
	}

	s.mu.Lock()
	s.clients[client] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, client)
		s.mu.Unlock()
		conn.Close()
	}()

	go s.writePump(client)

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("admin ws read error: %v", err)
			}
			break
		}
	}
}

func (s *Server) writePump(client *adminClient) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		client.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-client.send:
			client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := client.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(msg)
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			client.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (s *Server) broadcastLoop() {
	for evt := range s.alertCh {
		data, err := json.Marshal(evt)
		if err != nil {
			log.Printf("marshal alert event failed: %v", err)
			continue
		}
		s.mu.RLock()
		for client := range s.clients {
			select {
			case client.send <- data:
			default:
				close(client.send)
				delete(s.clients, client)
			}
		}
		s.mu.RUnlock()
	}
}

func (s *Server) Start(port int) error {
	ch := s.alertMgr.Subscribe()
	go func() {
		for evt := range ch {
			select {
			case s.alertCh <- evt:
			default:
				log.Printf("alert channel full, dropping event: %s", evt.RuleID)
			}
		}
	}()
	go s.broadcastLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("admin notifier server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
