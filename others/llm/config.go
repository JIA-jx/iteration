package llm

import (
	"context"
	"errors"
	"time"

	"test.com/others/circuit"
)

type Cache interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Put(ctx context.Context, key, value string, ttl time.Duration) error
}

type Config struct {
	APIKey           string
	BaseURL          string
	Model            string
	Timeout          time.Duration
	MaxRetries       int
	RetryBackoff     time.Duration
	MaxRetryBackoff  time.Duration
	BreakerName      string
	BreakerFailures  int
	BreakerOpenFor   time.Duration
	InputPricePer1K  float64
	OutputPricePer1K float64
	Cache            Cache
	CacheTTL         time.Duration
	MaxPromptChars   int
	OnUsage          func(ctx context.Context, model string, inputTokens, outputTokens int)
}

const defaultBaseURL = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"

func (c *Config) withDefaults() {
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	if c.Model == "" {
		c.Model = "qwen-turbo"
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 300 * time.Millisecond
	}
	if c.MaxRetryBackoff <= 0 {
		c.MaxRetryBackoff = 5 * time.Second
	}
	if c.BreakerName == "" {
		c.BreakerName = "llm"
	}
	if c.BreakerFailures <= 0 {
		c.BreakerFailures = 5
	}
	if c.BreakerOpenFor <= 0 {
		c.BreakerOpenFor = 30 * time.Second
	}
	if c.CacheTTL <= 0 {
		c.CacheTTL = 10 * time.Minute
	}
	if c.MaxPromptChars <= 0 {
		c.MaxPromptChars = 12000
	}
}

func (c *Config) breakerOptions() circuit.Options {
	return circuit.Options{
		Name:        c.BreakerName,
		MaxFailures: c.BreakerFailures,
		OpenTimeout: c.BreakerOpenFor,
		HalfOpenMax: 1,
		MinRequests: 5,
		FailureRate: 0.6,
		Window:      20,
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return true
			}
			return false
		},
	}
}
