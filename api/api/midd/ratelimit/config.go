package ratelimit

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

const maxLimiterEntries = 20000

type Config struct {
	Gateway GatewayConfig `yaml:"gateway"`
}

type GatewayConfig struct {
	RateLimit RateLimitConfig `yaml:"rate_limit"`
}

type RateLimitConfig struct {
	Global RuleConfig            `yaml:"global"`
	APIs   map[string]RuleConfig `yaml:"apis"`
}

type RuleConfig struct {
	Algorithm        string  `yaml:"algorithm"`
	MaxTokens        int     `yaml:"max_tokens"`
	TokenRate        int     `yaml:"token_rate"`
	DegradeThreshold float64 `yaml:"degrade_threshold"`
	DegradeAction    string  `yaml:"degrade_action"`
	ErrorCode        int     `yaml:"error_code"`
	ErrorMsg         string  `yaml:"error_msg"`
	RedirectURL      string  `yaml:"redirect_url"`
}

type RateLimitMiddleware struct {
	cfg      *Config
	limiters map[string]Limiter
	mu       sync.Mutex
}

func NewRateLimitMiddleware(cfgPath string) (*RateLimitMiddleware, error) {
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		return nil, err
	}

	return &RateLimitMiddleware{
		cfg:      cfg,
		limiters: make(map[string]Limiter),
	}, nil
}

func LoadConfig(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var cfg Config

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (m *RateLimitMiddleware) getRuleAndLimiter(path string, clientKey string) (RuleConfig, Limiter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rule, ok := m.cfg.Gateway.RateLimit.APIs[path]
	if !ok {
		rule = m.cfg.Gateway.RateLimit.Global
		path = "_global_"
	}

	key := path + "|" + clientKey
	if _, exists := m.limiters[key]; !exists {
		if len(m.limiters) >= maxLimiterEntries {
			m.limiters = make(map[string]Limiter)
		}
		m.limiters[key] = NewLimiter(rule)
	}

	return rule, m.limiters[key]
}

func clientKey(c *gin.Context) string {
	if memberId := c.GetInt64("memberId"); memberId > 0 {
		return "m:" + strconv.FormatInt(memberId, 10)
	}
	return "ip:" + c.ClientIP()
}

var (
	defaultMu  sync.RWMutex
	defaultRLM *RateLimitMiddleware
)

func SetDefault(m *RateLimitMiddleware) {
	defaultMu.Lock()
	defaultRLM = m
	defaultMu.Unlock()
}

func Middleware() gin.HandlerFunc {
	defaultMu.RLock()
	m := defaultRLM
	defaultMu.RUnlock()
	if m == nil {
		return func(c *gin.Context) { c.Next() }
	}
	return m.GinMiddleware()
}

func statusCode(rule RuleConfig) int {
	if rule.ErrorCode >= 100 && rule.ErrorCode <= 999 {
		return rule.ErrorCode
	}
	return http.StatusServiceUnavailable
}

func (m *RateLimitMiddleware) degrade(w http.ResponseWriter, rule RuleConfig) {
	code := statusCode(rule)
	msg := rule.ErrorMsg
	if msg == "" {
		msg = "服务繁忙，请稍后重试"
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	switch rule.DegradeAction {
	case "return_empty":
		resp := map[string]interface{}{
			"code": 0,
			"msg":  "服务临时降级",
			"data": nil,
		}
		json.NewEncoder(w).Encode(resp)

	case "redirect":
		if rule.RedirectURL != "" {
			w.Header().Set("Location", rule.RedirectURL)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"code": code,
			"msg":  msg,
			"data": nil,
		})

	default:
		w.WriteHeader(code)
		resp := map[string]interface{}{
			"code": code,
			"msg":  msg,
			"data": nil,
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func bearerToken(raw string) string {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(raw[len("Bearer "):])
}

func (m *RateLimitMiddleware) UnauthMiddleware(memberIdFromToken func(token string) (int64, error)) gin.HandlerFunc {
	return func(c *gin.Context) {
		if memberIdFromToken != nil {
			if token := bearerToken(c.GetHeader("Authorization")); token != "" {
				if id, err := memberIdFromToken(token); err == nil && id > 0 {
					c.Next()
					return
				}
			}
		}

		rule, limiter := m.getRuleAndLimiter(c.Request.URL.Path, "ip:"+c.ClientIP())
		allowed, usage := limiter.Allow()
		if !allowed || usage >= rule.DegradeThreshold {
			m.degrade(c.Writer, rule)
			c.Abort()
			return
		}

		c.Next()
	}
}

func (m *RateLimitMiddleware) GinMiddleware() func(c *gin.Context) {
	return func(c *gin.Context) {
		path := c.Request.URL.Path
		rule, limiter := m.getRuleAndLimiter(path, clientKey(c))

		allowed, usage := limiter.Allow()
		if !allowed || usage >= rule.DegradeThreshold {
			m.degrade(c.Writer, rule)
			c.Abort()
			return
		}

		c.Next()
	}
}
