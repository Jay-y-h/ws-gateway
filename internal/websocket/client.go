package websocket

import (
	"encoding/json"
	"log/slog"
	"strings"
	"ws-gateway/internal/protocol"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn *websocket.Conn
	server *Server
	//device/browser
	clientType string

}

func NewClient(
	conn *websocket.Conn,
	server *Server,
)*Client{

	return &Client{
		conn:conn,
		server:server,
	}
}

func (c *Client) ReadLoop(){
	defer func(){
		c.server.unregister<-c
	}()

	for {
		_,data,err:=c.conn.ReadMessage()
		if err!=nil{
			slog.Error(
				"读取失败",
				"error",
				err,
			)
			break
		}
		slog.Info("收到ws数据","ws数据",data)

		var check struct{
			CellSN string `json:"cellSN"`
			TestTime string `json:"testTime"`
		}
		json.Unmarshal(data,&check)
		if check.CellSN!="" && check.TestTime!=""{
			
			c.server.browserBroadcast<-data
			continue
		}

		var msg protocol.Message

		err = json.Unmarshal(data,&msg)
		if err!=nil{
			slog.Error(
				"json解析失败",
				"error",
				err,
			)
			continue
		}
		

		if strings.Contains(msg.DevId,"dev"){
			slog.Info("设备数据","dev_id",msg.DevId)
			c.server.browserBroadcast<-data
			continue
		}
		slog.Info("browser命令","browser",msg.DevId)
		
		c.server.deviceBroadcast<-data
	}

}

func (c *Client) Send(
	data []byte,
){

	err:=c.conn.WriteMessage(
		websocket.TextMessage,
		data,
	)

	if err!=nil{

		slog.Error(
			"发送失败",
			"error",
			err,
		)
	}

}