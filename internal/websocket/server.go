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
	// 所有 WS 客户端
	clients map[*Client]bool
	// 根据 clientID 查找客户端
	clientByID map[string]*Client
	register   chan *Client
	unregister chan *Client
	// 所有消息统一从这里进入
	route chan *protocol.Message
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

		route: make(chan *protocol.Message),
	}
}

func (s *Server) Run() {
	for {
		select {
		case client := <-s.register:
			s.registerClient(client)
		case client := <-s.unregister:
			s.unregisterClient(client)
		case msg := <-s.route:
			s.routeMessage(msg)
		}
	}
}

func (s *Server) registerClient(
	client *Client,
) {
	// 同一个 ID 已经有一个连接在线：先把旧连接关掉再让新连接顶替，
	// 避免旧连接的 goroutine 泄漏，也避免旧连接后续断开时把新连接从
	// clientByID 里错误地删掉。
	if old, ok := s.clientByID[client.ID]; ok && old != client {
		slog.Warn(
			"同一 ID 重复连接，关闭旧连接",
			"id", client.ID,
		)
		s.closeClient(old)
	}

	s.clients[client] = true
	s.clientByID[client.ID] = client

	slog.Info(
		"客户端连接",
		"id", client.ID,
		"type", client.Type,
		"addr", client.conn.RemoteAddr(),
	)
}

func (s *Server) unregisterClient(
	client *Client,
) {
	// 只有当 clientByID 里存的仍然是这个 client 实例时才删除，
	// 防止旧连接的注销把新连接（同 ID 顶替上来的）从表里删掉。
	if cur, ok := s.clientByID[client.ID]; ok && cur == client {
		delete(s.clientByID, client.ID)
	}
	if _, ok := s.clients[client]; !ok {
		// 已经被清理过（例如被 registerClient 中的 closeClient
		// 处理过），避免重复关闭。
		return
	}
	s.closeClient(client)

	slog.Info(
		"客户端断开",
		"id", client.ID,
		"type", client.Type,
	)
}
// closeClient 做实际的资源清理：从 clients 表移除、关闭 send channel
// 和底层连接。只应该在持有单一事件循环（Run goroutine）时调用。
func (s *Server) closeClient(client *Client) {
	delete(s.clients, client)

	if !client.closed {
		client.closed = true
		close(client.send)
	}

	client.conn.Close()
}

func (s *Server) routeMessage(msg *protocol.Message) {
	slog.Info(
		"route msg",
		"from", msg.From,
		"to", msg.To,
	)
	if msg.To != "" {
		s.sendTo(msg.To, msg)
		return
	}

	// 没有 To，根据消息来源的 Client.Type（服务端在握手时记录，
	// 而不是靠猜测 ID 前缀）决定广播对象。
	fromClient, ok := s.clientByID[msg.From]
	if !ok {
		slog.Warn(
			"无法确定消息路由：来源客户端已不在线",
			"from", msg.From,
		)
		return
	}

	switch fromClient.Type {
	case "device":
		// 设备 → 所有浏览器
		s.broadcastByType("browser", msg)
	case "browser":
		// 浏览器 → 所有设备
		s.broadcastByType("device", msg)
	default:
		slog.Warn(
			"无法确定消息路由",
			"from", msg.From,
			"type", fromClient.Type,
		)
	}

	// 没有 To，根据消息来源决定广播对象
	// if strings.HasPrefix(msg.From, "dev") {
	// 	// 设备 → 所有浏览器
	// 	s.broadcastByType(
	// 		"browser",
	// 		msg,
	// 	)
	// 	return
	// }
	// if strings.HasPrefix(msg.From, "brow") {
	// 	// 浏览器 → 所有设备
	// 	s.broadcastByType(
	// 		"device",
	// 		msg,
	// 	)
	// 	return
	// }
	// slog.Warn(
	// 	"无法确定消息路由",
	// 	"from", msg.From,
	// )
}

func (s *Server) sendTo(
	id string,
	msg *protocol.Message,
) {
	client, ok := s.clientByID[id]

	if !ok {
		slog.Warn(
			"目标不存在",
			"to", id,
		)
		return
	}

	data, err := json.Marshal(msg)
	if err != nil {
		slog.Error(
			"消息序列化失败",
			"error", err,
		)
		return
	}

	// client.send <- data

	// 非阻塞发送：Run() 是整个网关唯一的事件循环，这里绝不能被某个
	// 慢客户端卡住，否则所有客户端的注册/注销/路由都会被拖死。
	if err := client.enqueue(data); err != nil {
		slog.Error(
			"发送失败，客户端缓冲已满",
			"client", client.ID,
			"error", err,
		)
	}
}

func (s *Server) broadcastByType(
	clientType string,
	msg *protocol.Message,
) {

	for client := range s.clients {
		if client.Type != clientType {
			continue
		}
		if err := client.Send(msg); err != nil {
			slog.Error(
				"广播失败",
				"client", client.ID,
				"error", err,
			)
		}
	}
}

func (s *Server) Handler(
	w http.ResponseWriter,
	r *http.Request,
) {
	query := r.URL.Query()
	clientType := query.Get("type")
	clientID := query.Get("id")

	if clientType == "" || clientID == "" {
		http.Error(w, "missing id or type", http.StatusBadRequest)
		slog.Warn("missing websocket client info")
		return
	}

	conn, err := s.upgrader.Upgrade(
		w,
		r,
		nil,
	)

	if err != nil {
		slog.Error(
			"websocket upgrade failed",
			"error", err,
		)
		return
	}


	client := NewClient(
		conn,
		s,
	)
	client.ID = clientID
	client.Type = clientType
	s.register <- client
	go client.WriteLoop()
	go client.ReadLoop()
}