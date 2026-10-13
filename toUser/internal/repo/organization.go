package repo

import (
	"context"
	"test.com/toUser/internal/data/organization"
	"test.com/toUser/internal/database"
)

type OrganizationRepo interface {
	SaveOrganization(conn database.DbConn, ctx context.Context, org *organization.Organization) error
	FindOrganizationByMemId(ctx context.Context, memId int64) ([]*organization.Organization, error)
	FindOrgListByMemberId(ctx context.Context, memberId int64) ([]*organization.Organization, error)
}
