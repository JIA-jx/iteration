package data

type AIResult struct {
	Suggest    string
	Analysis   string
	WorkHour   int
	RemainHour int
}

type DeliveryResult struct {
	Total     int64
	Completed int64
	Doing     int64
	Overdue   int64
	Rate      float64
}

type BurndownDailyDoneStat struct {
	DayUnix int64
	Count   int64
}

type MemberTaskStat struct {
	MemberCode int64  `gorm:"column:member_code"`
	Name       string `gorm:"column:name"`
	Avatar     string `gorm:"column:avatar"`
	TaskCount  int64  `gorm:"column:task_count"`
	WorkHour   int64  `gorm:"column:work_hour"`
	TenantId   int64  `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*MemberTaskStat) TableName() string {
	return "iteration_member_task_stat"
}
