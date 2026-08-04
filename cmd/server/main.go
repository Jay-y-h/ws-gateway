package main

import (
	"log"
	"net/http"
	"ws-gateway/internal/websocket"
)

func main() {
	wsServer := websocket.NewServer()
	go wsServer.Run()

	http.HandleFunc("/ws",wsServer.Handler)

	log.Println(
		"WebSocket server :4001",
	)

	err:=http.ListenAndServe(
		":4001",
		nil,
	)

	if err!=nil{
		log.Fatal(err)
	}

}