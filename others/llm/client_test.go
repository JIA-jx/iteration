package llm

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type payload struct {
	Suggest  string `json:"Suggest"`
	WorkHour int    `json:"WorkHour"`
}

func TestDecodeJSONHandlesFencedAndPrefixedOutput(t *testing.T) {
	cases := map[string]string{
		"纯 json":       `{"Suggest":"a","WorkHour":1}`,
		"markdown 代码块": "```json\n{\"Suggest\":\"a\",\"WorkHour\":1}\n```",
		"无语言标记代码块":     "```\n{\"Suggest\":\"a\",\"WorkHour\":1}\n```",
		"前后有解释文字":      `好的，结果如下：{"Suggest":"a","WorkHour":1} 希望有帮助`,
		"嵌套对象":         `{"Suggest":"a","WorkHour":1,"extra":{"x":1}}`,
	}

	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			var out payload
			if err := decodeJSON(in, &out); err != nil {
				t.Fatalf("decodeJSON 失败: %v", err)
			}
			if out.Suggest != "a" || out.WorkHour != 1 {
				t.Fatalf("解析结果异常: %+v", out)
			}
		})
	}
}

func TestDecodeJSONRejectsGarbage(t *testing.T) {
	var out payload
	if err := decodeJSON("完全没有 json", &out); err == nil {
		t.Fatalf("没有 json 对象时应报错")
	}
	if err := decodeJSON(`{"Suggest":`, &out); err == nil {
		t.Fatalf("非法 json 应报错")
	}
}

type validatingPayload struct {
	Suggest string `json:"Suggest"`
}

func (v *validatingPayload) Validate() error {
	if v.Suggest == "" {
		return errors.New("suggest required")
	}
	return nil
}

func TestDecodeJSONRunsValidator(t *testing.T) {
	var ok validatingPayload
	if err := decodeJSON(`{"Suggest":"x"}`, &ok); err != nil {
		t.Fatalf("校验应通过: %v", err)
	}

	var bad validatingPayload
	if err := decodeJSON(`{"Suggest":""}`, &bad); err == nil {
		t.Fatalf("校验失败应返回错误")
	}
}

type stubCaller struct {
	responses []string
	calls     int
	err       error
}

func (s *stubCaller) Call(ctx context.Context, prompt string) (*Response, error) {
	return nil, errors.New("not used")
}

func (s *stubCaller) CallJSON(ctx context.Context, prompt string, out interface{}) error {
	return nil
}

func TestPromptRegistryRender(t *testing.T) {
	if err := RegisterPrompt(PromptTemplate{
		Name:         "t_render",
		Version:      "v1",
		Template:     "hello {{name}}, you have {{count}} tasks",
		RequiredVars: []string{"name", "count"},
	}); err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	got, err := Render("t_render", "v1", map[string]string{"name": "bob", "count": "3"})
	if err != nil {
		t.Fatalf("渲染失败: %v", err)
	}
	if got != "hello bob, you have 3 tasks" {
		t.Fatalf("渲染结果 = %q", got)
	}

	if _, err := Render("t_render", "v1", map[string]string{"name": "bob"}); err == nil {
		t.Fatalf("缺少必填变量时应报错")
	}
	if _, err := Render("t_missing", "v1", nil); err == nil {
		t.Fatalf("未注册的 prompt 应报错")
	}
}

func TestPromptRegistryValidation(t *testing.T) {
	if err := RegisterPrompt(PromptTemplate{Name: "", Template: "x"}); err == nil {
		t.Fatalf("空名字应报错")
	}
	if err := RegisterPrompt(PromptTemplate{Name: "x", Template: "   "}); err == nil {
		t.Fatalf("空模板应报错")
	}
}

func TestLatestVersionAndList(t *testing.T) {
	for _, v := range []string{"v1", "v3", "v2"} {
		if err := RegisterPrompt(PromptTemplate{Name: "t_ver", Version: v, Template: "x"}); err != nil {
			t.Fatalf("注册 %s 失败: %v", v, err)
		}
	}

	if got := LatestVersion("t_ver"); got != "v3" {
		t.Fatalf("LatestVersion = %s, want v3", got)
	}
	if got := LatestVersion("t_none"); got != "" {
		t.Fatalf("不存在的 prompt 应返回空串, got %s", got)
	}

	list := ListPrompts()
	if len(list) < 4 {
		t.Fatalf("ListPrompts 数量异常: %d", len(list))
	}
	for i := 1; i < len(list); i++ {
		if list[i-1].Name > list[i].Name {
			t.Fatalf("ListPrompts 未按名称排序")
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	cfg.withDefaults()

	if cfg.Model != "qwen-turbo" {
		t.Fatalf("默认模型 = %s", cfg.Model)
	}
	if cfg.MaxRetries != 3 {
		t.Fatalf("默认重试 = %d", cfg.MaxRetries)
	}
	if cfg.BreakerFailures != 5 {
		t.Fatalf("默认熔断阈值 = %d", cfg.BreakerFailures)
	}
	if !strings.Contains(cfg.BaseURL, "dashscope") {
		t.Fatalf("默认 BaseURL 异常: %s", cfg.BaseURL)
	}

	opts := cfg.breakerOptions()
	if opts.Name != "llm" || opts.MaxFailures != 5 {
		t.Fatalf("熔断配置映射异常: %+v", opts)
	}
}

func TestStripCodeFence(t *testing.T) {
	if got := stripCodeFence("```json\n{}\n```"); got != "{}" {
		t.Fatalf("stripCodeFence = %q", got)
	}
	if got := stripCodeFence("{}"); got != "{}" {
		t.Fatalf("无代码块时不应改动, got %q", got)
	}
}

func TestIsRetryableClassification(t *testing.T) {
	if !isRetryable(&httpError{Status: 429}) {
		t.Fatalf("429 应重试")
	}
	if !isRetryable(&httpError{Status: 503}) {
		t.Fatalf("5xx 应重试")
	}
	if isRetryable(&httpError{Status: 400}) {
		t.Fatalf("4xx 不应重试")
	}
	if isRetryable(context.Canceled) {
		t.Fatalf("上下文取消不应重试")
	}
	if !isRetryable(ErrEmptyResult) {
		t.Fatalf("空结果应重试")
	}

	if got := classify(&httpError{Status: 429}); got != "rate_limited" {
		t.Fatalf("classify = %s", got)
	}
	if got := classify(&httpError{Status: 500}); got != "server_error" {
		t.Fatalf("classify = %s", got)
	}
	if got := classify(errors.New("dial tcp")); got != "network" {
		t.Fatalf("classify = %s", got)
	}
}

func TestCallRejectsMissingKeyAndEmptyPrompt(t *testing.T) {
	c := NewClient(Config{})
	if _, err := c.Call(context.Background(), "hi"); !errors.Is(err, ErrEmptyAPIKey) {
		t.Fatalf("缺少 api key 应返回 ErrEmptyAPIKey, got %v", err)
	}

	c2 := NewClient(Config{APIKey: "k"})
	if _, err := c2.Call(context.Background(), "   "); !errors.Is(err, ErrEmptyPrompt) {
		t.Fatalf("空 prompt 应返回 ErrEmptyPrompt, got %v", err)
	}
}
