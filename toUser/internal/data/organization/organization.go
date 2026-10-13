package organization

type Organization struct {
	Id          int64
	Name        string
	Avatar      string
	Description string
	MemberId    int64
	CreateTime  int64
	Personal    int32
	Address     string
	Province    int32
	City        int32
	Area        int32
	TenantId    int64
}

func (*Organization) TableName() string {
	return "iteration_organization"
}
