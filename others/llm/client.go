package llm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"test.com/others/circuit"
)

var (
	ErrEmptyAPIKey = errors.New("llm: api key is not configured")
	ErrEmptyPrompt = errors.New("llm: prompt is empty")
	ErrBreakerOpen = errors.New("llm: circuit breaker is open")
	ErrEmptyResult = errors.New("llm: empty result from provider")
)

type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type Response struct {
	Content   string
	Model     string
	Usage     Usage
	Attempts  int
	FromCache bool
	Latency   time.Duration
}

type Client struct {
	cfg     Config
	http    *http.Client
	breaker *circuit.Breaker
}

func NewClient(cfg Config) *Client {
	cfg.withDefaults()
	return &Client{
		cfg:     cfg,
		http:    &http.Client{Timeout: cfg.Timeout},
		breaker: circuit.New(cfg.breakerOptions()),
	}
}

func (c *Client) BreakerState() circuit.State {
	return c.breaker.State()
}

func (c *Client) SetUsageHook(hook func(ctx context.Context, model string, inputTokens, outputTokens int)) {
	usageHookMu.Lock()
	c.cfg.OnUsage = hook
	usageHookMu.Unlock()
}

func (c *Client) Call(ctx context.Context, prompt string) (*Response, error) {
	if c.cfg.APIKey == "" {
		llmRejected.WithLabelValues("no_api_key").Inc()
		return nil, ErrEmptyAPIKey
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		llmRejected.WithLabelValues("empty_prompt").Inc()
		return nil, ErrEmptyPrompt
	}
	if len(prompt) > c.cfg.MaxPromptChars {
		prompt = truncateRunes(prompt, c.cfg.MaxPromptChars)
		llmRejected.WithLabelValues("prompt_truncated").Inc()
	}

	cacheKey := c.cacheKey(prompt)
	if c.cfg.Cache != nil {
		if cached, ok, err := c.cfg.Cache.Get(ctx, cacheKey); err == nil && ok {
			llmCache.WithLabelValues("hit").Inc()
			return &Response{Content: cached, Model: c.cfg.Model, FromCache: true}, nil
		}
		llmCache.WithLabelValues("miss").Inc()
	}

	var resp *Response
	err := c.breaker.Do(ctx, func(ctx context.Context) error {
		r, callErr := c.callWithRetry(ctx, prompt)
		if callErr != nil {
			return callErr
		}
		resp = r
		return nil
	})
	if err != nil {
		llmCalls.WithLabelValues(c.cfg.Model, "error").Inc()
		if errors.Is(err, circuit.ErrOpen) || errors.Is(err, circuit.ErrTooManyProbes) {
			return nil, fmt.Errorf("%w: %v", ErrBreakerOpen, err)
		}
		return nil, err
	}

	if c.cfg.Cache != nil && resp.Content != "" {
		if err := c.cfg.Cache.Put(ctx, cacheKey, resp.Content, c.cfg.CacheTTL); err != nil {
			_ = err
		}
	}

	return resp, nil
}

func (c *Client) CallJSON(ctx context.Context, prompt string, out interface{}) error {
	if out == nil {
		return errors.New("llm: out is nil")
	}

	resp, err := c.Call(ctx, prompt)
	if err != nil {
		return err
	}

	if decodeErr := decodeJSON(resp.Content, out); decodeErr == nil {
		return nil
	}

	repair := prompt + "\n\n注意：你上一次的回复不是合法 JSON。请只输出合法 JSON 对象，不要输出任何解释文字，不要使用 markdown 代码块。"
	llmRetries.WithLabelValues(c.cfg.Model, "json_repair").Inc()

	resetTarget(out)

	resp2, err2 := c.Call(ctx, repair)
	if err2 != nil {
		return err2
	}
	if decodeErr := decodeJSON(resp2.Content, out); decodeErr != nil {
		return fmt.Errorf("llm: decode json failed after repair: %w", decodeErr)
	}
	return nil
}

func resetTarget(out interface{}) {
	if out == nil {
		return
	}
	v := reflect.ValueOf(out)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return
	}
	elem := v.Elem()
	if elem.CanSet() {
		elem.Set(reflect.Zero(elem.Type()))
	}
}

func (c *Client) callWithRetry(ctx context.Context, prompt string) (*Response, error) {
	var lastErr error

	for attempt := 1; attempt <= c.cfg.MaxRetries; attempt++ {
		start := time.Now()
		resp, err := c.doRequest(ctx, prompt)
		elapsed := time.Since(start)
		llmLatency.WithLabelValues(c.cfg.Model).Observe(elapsed.Seconds())

		if err == nil {
			resp.Attempts = attempt
			resp.Latency = elapsed
			c.observeUsage(ctx, resp)
			llmCalls.WithLabelValues(c.cfg.Model, "ok").Inc()
			return resp, nil
		}

		lastErr = err
		if !isRetryable(err) {
			break
		}

		llmRetries.WithLabelValues(c.cfg.Model, classify(err)).Inc()

		backoff := c.cfg.RetryBackoff * time.Duration(int64(1)<<uint(attempt-1))
		if backoff > c.cfg.MaxRetryBackoff {
			backoff = c.cfg.MaxRetryBackoff
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	return nil, lastErr
}

type providerResponse struct {
	Output struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	} `json:"output"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	RequestId string `json:"request_id"`
}

type httpError struct {
	Status int
	Body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("llm: http status %d: %s", e.Status, e.Body)
}

func (c *Client) doRequest(ctx context.Context, prompt string) (*Response, error) {
	body, err := json.Marshal(map[string]interface{}{
		"model": c.cfg.Model,
		"input": map[string]interface{}{
			"messages": []map[string]interface{}{
				{"role": "user", "content": prompt},
			},
		},
		"parameters": map[string]interface{}{
			"result_format": "json",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("llm: build request failed: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("llm: create request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)

	httpResp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("llm: request failed: %w", err)
	}
	defer httpResp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(httpResp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("llm: read response failed: %w", err)
	}

	if httpResp.StatusCode != http.StatusOK {
		return nil, &httpError{Status: httpResp.StatusCode, Body: truncate(string(raw), 512)}
	}

	var parsed providerResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("llm: parse response failed: %w", err)
	}

	if parsed.Error.Message != "" {
		return nil, fmt.Errorf("llm: provider error %s: %s", parsed.Error.Code, parsed.Error.Message)
	}
	if len(parsed.Output.Choices) == 0 {
		return nil, ErrEmptyResult
	}

	content := parsed.Output.Choices[0].Message.Content
	if strings.TrimSpace(content) == "" {
		return nil, ErrEmptyResult
	}

	return &Response{
		Content: content,
		Model:   c.cfg.Model,
		Usage: Usage{
			InputTokens:  parsed.Usage.InputTokens,
			OutputTokens: parsed.Usage.OutputTokens,
			TotalTokens:  parsed.Usage.TotalTokens,
		},
	}, nil
}

func (c *Client) observeUsage(ctx context.Context, resp *Response) {
	if resp.Usage.InputTokens > 0 {
		llmTokens.WithLabelValues(c.cfg.Model, "input").Add(float64(resp.Usage.InputTokens))
	}
	if resp.Usage.OutputTokens > 0 {
		llmTokens.WithLabelValues(c.cfg.Model, "output").Add(float64(resp.Usage.OutputTokens))
	}

	if c.cfg.InputPricePer1K > 0 || c.cfg.OutputPricePer1K > 0 {
		cost := float64(resp.Usage.InputTokens)/1000*c.cfg.InputPricePer1K +
			float64(resp.Usage.OutputTokens)/1000*c.cfg.OutputPricePer1K
		if cost > 0 {
			llmCost.WithLabelValues(c.cfg.Model).Add(cost)
		}
	}

	if hook := c.usageHook(); hook != nil {
		hook(ctx, c.cfg.Model, resp.Usage.InputTokens, resp.Usage.OutputTokens)
	}
}

var (
	usageHookMu      sync.RWMutex
	defaultUsageHook func(ctx context.Context, model string, inputTokens, outputTokens int)
)

func (c *Client) usageHook() func(ctx context.Context, model string, inputTokens, outputTokens int) {
	usageHookMu.RLock()
	defer usageHookMu.RUnlock()

	if c.cfg.OnUsage != nil {
		return c.cfg.OnUsage
	}
	return defaultUsageHook
}

func SetDefaultUsageHook(h func(ctx context.Context, model string, inputTokens, outputTokens int)) {
	usageHookMu.Lock()
	defaultUsageHook = h
	usageHookMu.Unlock()
}

func (c *Client) cacheKey(prompt string) string {
	sum := sha256.Sum256([]byte(c.cfg.Model + "\x00" + prompt))
	return "llm:cache:" + hex.EncodeToString(sum[:])
}

func decodeJSON(content string, out interface{}) error {
	cleaned := stripCodeFence(content)

	start := strings.Index(cleaned, "{")
	end := strings.LastIndex(cleaned, "}")
	if start < 0 || end <= start {
		return errors.New("llm: no json object found in response")
	}
	cleaned = cleaned[start : end+1]

	if err := json.Unmarshal([]byte(cleaned), out); err != nil {
		return err
	}

	if v, ok := out.(interface{ Validate() error }); ok {
		return v.Validate()
	}
	return nil
}

func stripCodeFence(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}

	s = strings.TrimPrefix(s, "```")
	if idx := strings.Index(s, "\n"); idx >= 0 {
		s = s[idx+1:]
	}
	if idx := strings.LastIndex(s, "```"); idx >= 0 {
		s = s[:idx]
	}
	return strings.TrimSpace(s)
}

func isRetryable(err error) bool {
	var he *httpError
	if errors.As(err, &he) {
		if he.Status == http.StatusTooManyRequests {
			return true
		}
		return he.Status >= 500
	}
	if errors.Is(err, ErrEmptyResult) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

func classify(err error) string {
	var he *httpError
	if errors.As(err, &he) {
		switch {
		case he.Status == http.StatusTooManyRequests:
			return "rate_limited"
		case he.Status >= 500:
			return "server_error"
		default:
			return "client_error"
		}
	}
	if errors.Is(err, ErrEmptyResult) {
		return "empty_result"
	}
	return "network"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return truncateRunes(s, n)
}

func truncateRunes(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return string([]rune(s)[:maxRunes])
}
