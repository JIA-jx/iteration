package data

type File struct {
	Id               int64
	PathName         string
	Title            string
	Extension        string
	Size             int
	ObjectType       string
	OrganizationCode int64
	TaskCode         int64
	ProjectCode      int64
	CreateBy         int64
	CreateTime       int64
	Downloads        int
	Extra            string
	Deleted          int
	FileUrl          string
	FileType         string
	DeletedTime      int64
	TenantId         int64 `gorm:"column:tenant_id;not null;default:1;index" json:"tenant_id"`
}

func (*File) TableName() string {
	return "iteration_file"
}
