package websocket

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

type Server struct {

	upgrader websocket.Upgrader

	clients map[*Client]bool

	register chan *Client

	unregister chan *Client

	deviceBroadcast chan []byte
	browserBroadcast chan []byte
}

func NewServer() *Server {

	return &Server{

		upgrader: websocket.Upgrader{

			CheckOrigin: func(
				r *http.Request,
			) bool {
				return true
			},
		},

		clients: make(map[*Client]bool),

		register: make(chan *Client),

		unregister: make(chan *Client),

		deviceBroadcast: make(chan []byte),
		browserBroadcast: make(chan []byte),

	}
}


func (s *Server) Run(){

	for {
		select {
		case client:=<-s.register:
			s.clients[client]=true
			slog.Info(
				"设备连接",
				"addr",
				client.conn.RemoteAddr(),
			)
		case client:=<-s.unregister:
			delete(
				s.clients,
				client,
			)
			client.conn.Close()
		case data:=<-s.browserBroadcast:
			for c:=range s.clients{
				if c.clientType=="browser"{
					c.Send(data)
				}
			}
		case data:=<-s.deviceBroadcast:
			for c:=range s.clients{
				if c.clientType=="device"{
					c.Send(data)
				}
			}
		}
	}
}

func (s *Server) Handler(
	w http.ResponseWriter,
	r *http.Request,
){
	conn,err:=s.upgrader.Upgrade(
		w,
		r,
		nil,
	)

	if err!=nil{
		return
	}

	client:=NewClient(
		conn,
		s,
	)

	//解析URL参数
	query:=r.URL.Query()

	client.clientType = query.Get("type")
	

	s.register<-client

	go client.ReadLoop()

}