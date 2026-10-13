package event

type Event interface {
	Topic() string
}

type TaskExchangeEvent struct {
	TaskId      int64  `json:"task_id"`
	ProjectCode string `json:"project_code"`
	Name        string `json:"name"`
	AssignTo    int64  `json:"assign_to"`
	Status      int32  `json:"status"`
	CreatedAt   int64  `json:"created_at"`
}

func (TaskExchangeEvent) Topic() string { return "task_exchange" }

type WorkflowAuthApplyEvent struct {
	Biz     string   `json:"biz"`
	AuthId  string   `json:"auth_id"`
	Nodes   []string `json:"nodes"`
	Status  string   `json:"status"`
	EventAt string   `json:"event_at"`
}

func (WorkflowAuthApplyEvent) Topic() string { return "workflow_auth_apply" }

type AnalysisDeliveryEvent struct {
	ProjectCode string  `json:"project_code"`
	Total       int64   `json:"total"`
	Completed   int64   `json:"completed"`
	Rate        float64 `json:"rate"`
	Ts          int64   `json:"ts"`
}

func (AnalysisDeliveryEvent) Topic() string { return "analysis_delivery" }

type AnalysisBurndownEvent struct {
	ProjectCode string `json:"project_code"`
	PointCount  int    `json:"point_count"`
	Ts          int64  `json:"ts"`
}

func (AnalysisBurndownEvent) Topic() string { return "analysis_burndown" }

type AnalysisTeamLoadEvent struct {
	ProjectCode string `json:"project_code"`
	MemberCount int    `json:"member_count"`
	Ts          int64  `json:"ts"`
}

func (AnalysisTeamLoadEvent) Topic() string { return "analysis_teamload" }

type AnalysisAIRecommendEvent struct {
	TaskCode  string  `json:"task_code"`
	Project   int64   `json:"project"`
	Suggest   string  `json:"suggest"`
	Risk      string  `json:"risk"`
	Members   []int64 `json:"members"`
	CreatedAt int64   `json:"created_at"`
}

func (AnalysisAIRecommendEvent) Topic() string { return "analysis_ai_recommend" }

type DelayNotifyEvent struct {
	TaskId      string `json:"task_id"`
	Type        string `json:"type"`
	BizId       int64  `json:"biz_id"`
	ProjectCode int64  `json:"project_code"`
	ToMemberId  int64  `json:"to_member_id"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	Ts          int64  `json:"ts"`
}

func (DelayNotifyEvent) Topic() string { return "delay_notify" }

type InviteExpiredEvent struct {
	InviteId    string `json:"invite_id"`
	ProjectCode int64  `json:"project_code"`
	InviterId   int64  `json:"inviter_id"`
	ExpiredAt   int64  `json:"expired_at"`
	Ts          int64  `json:"ts"`
}

func (InviteExpiredEvent) Topic() string { return "invite_expired" }
