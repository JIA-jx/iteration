package data

type SystemNotify struct {
	Id        int64  `json:"id"`
	Title     string `json:"title"`
	Content   string `json:"content"`
	Type      string `json:"type"`
	CreatedAt string `json:"created_at"`
}

type SystemLog struct {
	TraceId   string `json:"trace_id"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	CreatedAt string `json:"created_at"`
}

type SystemConfig struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
