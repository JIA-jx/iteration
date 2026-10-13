package common

import (
	"context"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Run(r *gin.Engine, srvName string, addr string, stop func()) {
	srv := &http.Server{Addr: addr, Handler: r}

	go func() {
		log.Printf("%s running on %s", srvName, addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("%s failed: %v", srvName, err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("Shutting down %s...", srvName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("%s shutdown error: %v", srvName, err)
	}

	if stop != nil {
		done := make(chan struct{})
		go func() {
			stop()
			close(done)
		}()
		select {
		case <-done:
			log.Printf("%s cleanup done", srvName)
		case <-ctx.Done():
			log.Printf("%s cleanup timeout", srvName)
		}
	}

	log.Printf("%s stopped", srvName)
}
