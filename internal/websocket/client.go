package websocket

import (
	"encoding/json"
	"log/slog"
	"ws-gateway/internal/protocol"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn *websocket.Conn
	server *Server
	ID string
	Type string
	// 写消息的 channel
	send chan []byte
	// closed 标记该 client 是否已经被关闭/注销，避免重复 close(send)
	closed bool
}
func NewClient(
	conn *websocket.Conn,
	server *Server,
)*Client{
	return &Client{
		conn:   conn,
		server: server,
		send:   make(chan []byte, 256),
	}
}

func (c *Client) ReadLoop() {
	defer func() {
		c.server.unregister <- c
	}()

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(
				err,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
			) {
				slog.Warn(
					"读取 websocket 失败",
					"id", c.ID,
					"error", err,
				)
			}
			return
		}

		slog.Info("收到ws数据", "ws数据", data)

		var msg protocol.Message

		if err := json.Unmarshal(data, &msg); err != nil {
			slog.Warn(
				"无效消息",
				"id", c.ID,
				"error", err,
			)
			continue
		}

		// 不相信客户端自己传的 From
		msg.From = c.ID

		slog.Info("即将发送数据", "数据", msg)

		c.server.route <- routeMessage{
			client: c,
			msg:    &msg,
		}
	}
}


func (c *Client) WriteLoop() {
	defer c.conn.Close()
	for data := range c.send {
		err := c.conn.WriteMessage(
			websocket.TextMessage,
			data,
		)
		if err != nil {
			slog.Warn(
				"发送 websocket 失败",
				"id", c.ID,
				"error", err,
			)
			return
		}
	}
}

func (c *Client) Send(msg *protocol.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return c.enqueue(data)
}


func (c *Client) enqueue(data []byte) error {
	select {
	case c.send <- data:
		return nil
	default:
		slog.Warn(
			"客户端发送缓冲已满，判定为异常连接，触发断开",
			"id", c.ID,
		)
		// 非阻塞地通知注销；如果 unregister 也满/慢，丢弃即可，
		// 不能阻塞在这里。
		select {
		case c.server.unregister <- c:
		default:
		}
		return websocket.ErrCloseSent
	}
}