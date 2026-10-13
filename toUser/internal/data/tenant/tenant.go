package tenant

type Tenant struct {
	Id             int64
	Code           string
	Name           string
	Status         int
	MemberQuota    int
	ProjectQuota   int
	StorageQuotaMb int64
}

func (*Tenant) TableName() string {
	return "tenant"
}
