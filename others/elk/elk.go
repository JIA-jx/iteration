package elk

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/olivere/elastic/v7"
)

var cli *ElkClient

type ElkClient struct {
	e *elastic.Client
}

func InitELK(addr string) {
	c, err := elastic.NewClient(
		elastic.SetURL(addr),
		elastic.SetSniff(false),
		elastic.SetHttpClient(&http.Client{Timeout: 3 * time.Second}),
	)
	if err != nil {
		log.Printf("初始化 ELK 失败，日志写入功能停用: %v", err)
		return
	}
	cli = &ElkClient{e: c}
}

func GetClient() *ElkClient {
	return cli
}

func (client *ElkClient) Index(index, data string) {
	if client == nil || client.e == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, _ = client.e.Index().
		Index(index).
		BodyJson(data).
		Do(ctx)
}
