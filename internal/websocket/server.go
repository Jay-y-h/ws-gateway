package websocket

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"ws-gateway/internal/protocol"

	"github.com/gorilla/websocket"
)

type Server struct {
	upgrader websocket.Upgrader

	clients    map[*Client]bool
	clientByID map[string]*Client

	register   chan *Client
	unregister chan *Client
	route      chan routeMessage
}

type routeMessage struct {
	client *Client
	msg    *protocol.Message
}

func NewServer() *Server {
	return &Server{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},

		clients:    make(map[*Client]bool),
		clientByID: make(map[string]*Client),

		register:   make(chan *Client),
		unregister: make(chan *Client),
		route:      make(chan routeMessage),
	}
}

func clientKey(typ, id string) string {
	return typ + ":" + id
}

func (s *Server) Run() {
	for {
		select {
		case client := <-s.register:
			s.registerClient(client)

		case client := <-s.unregister:
			s.unregisterClient(client)

		case route := <-s.route:
			s.routeMessage(route.client, route.msg)
		}
	}
}

func (s *Server) registerClient(client *Client) {
	key := clientKey(client.Type, client.ID)

	if old, ok := s.clientByID[key]; ok && old != client {
		slog.Warn("同一 ID 重复连接，关闭旧连接",
			"id", client.ID,
			"type", client.Type,
		)
		s.closeClient(old)
	}

	s.clients[client] = true
	s.clientByID[key] = client

	slog.Info("客户端连接",
		"id", client.ID,
		"type", client.Type,
		"addr", client.conn.RemoteAddr(),
	)
}

func (s *Server) unregisterClient(client *Client) {
	key := clientKey(client.Type, client.ID)

	if cur, ok := s.clientByID[key]; ok && cur == client {
		delete(s.clientByID, key)
	}

	if _, ok := s.clients[client]; !ok {
		return
	}

	s.closeClient(client)

	slog.Info("客户端断开",
		"id", client.ID,
		"type", client.Type,
	)
}

func (s *Server) closeClient(client *Client) {
	delete(s.clients, client)

	if !client.closed {
		client.closed = true
		close(client.send)
	}

	client.conn.Close()
}

func (s *Server) routeMessage(from *Client, msg *protocol.Message) {
	slog.Info("route msg",
		"from", msg.From,
		"to", msg.To,
		"type", from.Type,
	)

	// 指定目标
	if msg.To != "" {
		targetType := "device"
		if from.Type == "device" {
			targetType = "browser"
		}

		s.sendTo(targetType, msg.To, msg)
		return
	}

	// 不指定目标
	switch from.Type {
	case "device":
		// 设备 → 同一台电脑上的浏览器
		s.sendTo("browser", from.ID, msg)

	case "browser":
		// 浏览器 → 所有设备
		s.broadcastByType("device", msg)
	}
}

func (s *Server) sendTo(clientType, id string, msg *protocol.Message) {
	client, ok := s.clientByID[clientKey(clientType, id)]
	if !ok {
		slog.Warn("目标不存在",
			"type", clientType,
			"id", id,
		)
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error("消息序列化失败", "error", err)
		return
	}

	if err := client.enqueue(data); err != nil {
		slog.Error("发送失败",
			"client", client.ID,
			"error", err,
		)
	}
}

func (s *Server) broadcastByType(clientType string, msg *protocol.Message) {
	for client := range s.clients {
		if client.Type != clientType {
			continue
		}

		if err := client.Send(msg); err != nil {
			slog.Error("广播失败",
				"client", client.ID,
				"error", err,
			)
		}
	}
}

func (s *Server) Handler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	clientType := query.Get("type")
	clientID := query.Get("id")

	if clientType == "" || clientID == "" {
		http.Error(w, "missing id or type", http.StatusBadRequest)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("websocket upgrade failed", "error", err)
		return
	}

	client := NewClient(conn, s)
	client.ID = clientID
	client.Type = clientType

	s.register <- client

	go client.WriteLoop()
	go client.ReadLoop()
}