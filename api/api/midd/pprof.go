package midd

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func StartPprof() {
	go func() {
		log.Println("pprof 启动中")
		if err := http.ListenAndServe("127.0.0.1:8082", nil); err != nil {
			log.Println("pprof 启动失败:", err)
		}
	}()
}
