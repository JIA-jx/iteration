package llm

import (
	"context"
	"time"
)

type Caller interface {
	Call(ctx context.Context, prompt string) (*Response, error)
	CallJSON(ctx context.Context, prompt string, out interface{}) error
}

func WithDefaults(apiKey string, cache Cache) *Client {
	return NewClient(Config{
		APIKey:           apiKey,
		Model:            "qwen-turbo",
		Timeout:          30 * time.Second,
		MaxRetries:       3,
		RetryBackoff:     300 * time.Millisecond,
		MaxRetryBackoff:  5 * time.Second,
		BreakerName:      "llm",
		BreakerFailures:  5,
		BreakerOpenFor:   30 * time.Second,
		InputPricePer1K:  0.0008,
		OutputPricePer1K: 0.002,
		Cache:            cache,
		CacheTTL:         10 * time.Minute,
	})
}
