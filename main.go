package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"ws-gateway/internal/websocket"
)

func main() {
	wsServer := websocket.NewServer()
	go wsServer.Run()

	mux:=http.NewServeMux()
	mux.HandleFunc("/ws",wsServer.Handler)

	httpServer:=&http.Server{
		Addr:              ":4001",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second, // 防 slowloris 式握手攻击
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Println("WebSocket server :4001")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	

	// 优雅关闭：收到 SIGINT/SIGTERM 时给正在处理的连接一点时间收尾
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Println("shutdown error:", err)
	}
}