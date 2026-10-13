package login_service_v1

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"test.com/others/outbox"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"
	common "test.com/others"
	"test.com/others/encrypts"
	"test.com/others/errs"
	"test.com/others/jwts"
	"test.com/others/kk"
	"test.com/others/tenant"
	"test.com/others/tms"
	"test.com/rpcOrgrpc/user/login"
	"test.com/toUser/config"
	"test.com/toUser/internal/dao"
	"test.com/toUser/internal/data/member"
	"test.com/toUser/internal/data/organization"
	"test.com/toUser/internal/database"
	"test.com/toUser/internal/database/gorms"
	"test.com/toUser/internal/database/tran"
	"test.com/toUser/internal/event"
	"test.com/toUser/internal/repo"
	"test.com/toUser/pkg/model"
)

type LoginService struct {
	login.UnimplementedLoginServiceServer
	kafkaWriter      *kk.KafkaWriter
	cache            repo.Cache
	memberRepo       repo.MemberRepo
	organizationRepo repo.OrganizationRepo
	tenantDao        *dao.TenantDao
	transaction      tran.Transaction
	redSync          *redsync.Redsync
	outboxRepo       outbox.Repo

	accessPrivateKey  *rsa.PrivateKey
	refreshPrivateKey *rsa.PrivateKey
	publicKey         *rsa.PublicKey
}

const maxMemberIdsPerQuery = 200

func New() *LoginService {
	pool := goredis.NewPool(dao.Rc.GetClient())
	rs := redsync.New(pool)

	accessPrivateKey, err := jwts.LoadRSAPrivateKey("../api/api/midd/conf/rsa/rsa_private_key.pem")
	if err != nil {
		zap.L().Fatal("load access private key failed", zap.Error(err))
	}
	refreshPrivateKey, err := jwts.LoadRSAPrivateKey("../api/api/midd/conf/rsa/rsa_private_key.pem")
	if err != nil {
		zap.L().Fatal("load refresh private key failed", zap.Error(err))
	}
	publicKey, err := jwts.LoadRSAPublicKey("../api/api/midd/conf/rsa/rsa_public_key.pem")
	if err != nil {
		zap.L().Fatal("load public key failed", zap.Error(err))
	}

	return &LoginService{
		cache:            dao.Rc,
		kafkaWriter:      kk.GetWriter(config.C.KafkaConfig.Brokers[0]),
		memberRepo:       dao.NewMemberDao(),
		organizationRepo: dao.NewOrganizationDao(),
		tenantDao:        dao.NewTenantDao(),
		transaction:      dao.NewTransaction(),
		redSync:          rs,
		outboxRepo:       outbox.NewGormRepo(gorms.GetDB()),

		accessPrivateKey:  accessPrivateKey,
		refreshPrivateKey: refreshPrivateKey,
		publicKey:         publicKey,
	}
}

func tenantCodeFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, v := range md.Get("x-tenant-code") {
		if s := strings.TrimSpace(v); s != "" && s != tenant.DefaultCode {
			return s
		}
	}
	return ""
}

func GenerateRandomCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}

func (ls *LoginService) GetCaptcha(ctx context.Context, msg *login.CaptchaMessage) (*login.CaptchaResponse, error) {
	mobile := msg.Mobile
	if !common.VerifyMobile(mobile) {
		return nil, errs.GrpcError(model.NoLegalMobile)
	}

	lockKey := "lock:captcha:" + mobile
	mutex := ls.redSync.NewMutex(lockKey,
		redsync.WithExpiry(60*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("captcha frequent", zap.String("mobile", mobile))
		return nil, errs.GrpcError(model.CaptchaLimit)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	code, err := GenerateRandomCode()
	if err != nil {
		return nil, err
	}

	err = ls.cache.Put(ctx, model.RegisterRedisKey+mobile, code, 15*time.Minute)
	if err != nil {
		zap.L().Error("captcha save redis failed", zap.Error(err))
		return nil, errs.GrpcError(model.RedisError)
	}

	go func(c context.Context) {
		defer func() {
			if r := recover(); r != nil {
				zap.L().Error("sms panic", zap.Any("r", r), zap.Stack("stack"))
			}
		}()
		if config.C.SmsConfig != nil && config.C.SmsConfig.EchoCode {
			zap.L().Info("captcha generated (echo mode)", zap.String("mobile", mobile), zap.String("code", code))
			return
		}
		zap.L().Info("captcha dispatched", zap.String("mobile", mobile), zap.String("code", code))
	}(ctx)

	resp := &login.CaptchaResponse{}
	if config.C.SmsConfig != nil && config.C.SmsConfig.EchoCode {
		resp.Code = code
	}

	return resp, nil
}

func CaptchaLimit(ctx *gin.Context, mobile string) error {
	key := "limit:captcha:" + mobile
	if dao.Rc.Exist(ctx, key) {
		return errs.GrpcError(model.InterfaceLimit)
	}
	dao.Rc.Put(ctx, key, "1", 60*time.Second)
	return nil
}

func (ls *LoginService) Register(ctx context.Context, msg *login.RegisterMessage) (*login.RegisterResponse, error) {
	lockKey := "register:lock:" + msg.Mobile
	mutex := ls.redSync.NewMutex(lockKey,
		redsync.WithExpiry(10*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(ctx); err != nil {
		zap.L().Warn("register repeat", zap.String("mobile", msg.Mobile))
		return nil, errs.GrpcError(model.RegisterRepeat)
	}
	defer func() {
		_, _ = mutex.UnlockContext(ctx)
	}()

	captchaKey := model.RegisterRedisKey + msg.Mobile
	redisCode, err := ls.cache.Get(ctx, captchaKey)
	if err == redis.Nil {
		return nil, errs.GrpcError(model.CaptchaNotExist)
	}
	if err != nil {
		zap.L().Error("register captcha get redis err", zap.Error(err))
		return nil, errs.GrpcError(model.RedisError)
	}
	if redisCode != msg.Captcha {
		_ = ls.cache.Del(ctx, captchaKey)
		return nil, errs.GrpcError(model.CaptchaError)
	}

	if msg.Name == "" || msg.Password == "" || msg.Mobile == "" || msg.Email == "" {
		return nil, errs.GrpcError(model.ParamsError)
	}

	if exist, _ := ls.memberRepo.GetMemberByEmail(ctx, msg.Email); exist {
		return nil, errs.GrpcError(model.EmailExist)
	}
	if exist, _ := ls.memberRepo.GetMemberByAccount(ctx, msg.Name); exist {
		return nil, errs.GrpcError(model.AccountExist)
	}
	if exist, _ := ls.memberRepo.GetMemberByMobile(ctx, msg.Mobile); exist {
		return nil, errs.GrpcError(model.MobileExist)
	}

	pwd := encrypts.HashPassword(msg.Password, msg.Name)
	mem := &member.Member{
		Account:       msg.Name,
		Password:      pwd,
		Name:          msg.Name,
		Mobile:        msg.Mobile,
		Email:         msg.Email,
		CreateTime:    time.Now().UnixMilli(),
		LastLoginTime: time.Now().UnixMilli(),
		Status:        model.Normal,
	}

	if code := tenantCodeFromMetadata(ctx); code != "" {
		t, tErr := ls.tenantDao.FindByCode(ctx, code)
		if tErr != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if t == nil || t.Status != 1 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		if t.MemberQuota > 0 {
			used, cErr := ls.memberRepo.CountByTenant(ctx, t.Id)
			if cErr != nil {
				return nil, errs.GrpcError(model.DBError)
			}
			if used >= int64(t.MemberQuota) {
				return nil, errs.GrpcError(model.TenantQuotaError)
			}
		}
		mem.TenantId = t.Id
		ctx = tenant.WithContext(ctx, tenant.Info{Id: t.Id, Code: t.Code})
	}

	err = ls.transaction.Action(func(conn database.DbConn) error {
		if err := ls.memberRepo.SaveMember(conn, ctx, mem); err != nil {
			zap.L().Error("save member err", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}
		org := &organization.Organization{
			Name:       mem.Name + "个人组织",
			MemberId:   mem.Id,
			CreateTime: time.Now().UnixMilli(),
			Personal:   model.Personal,
			Avatar:     "https://b0.bdstatic.com/ugc/v9PvLJFU02y8jLj1kqiyJwa60e81cf61661a92d18a6761f364d3c5.jpg",
		}
		if err := ls.organizationRepo.SaveOrganization(conn, ctx, org); err != nil {
			zap.L().Error("save org err", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}

		tx := conn.(*gorms.GormConn).Tx(ctx)
		var defaultAuthId int64
		if err := tx.Table("iteration_auth").
			Where("is_default = ? AND status = ?", 1, 1).
			Where("tenant_id = ?", tenant.IdFromContext(ctx)).
			Order("id ASC").
			Limit(1).
			Pluck("id", &defaultAuthId).Error; err != nil {
			zap.L().Warn("load default auth role failed", zap.Error(err))
		}
		if defaultAuthId <= 0 {
			zap.L().Error("no default auth role for tenant", zap.Int64("tenant", tenant.IdFromContext(ctx)))
			return errs.GrpcError(model.TenantQuotaError)
		}

		now := time.Now().UnixMilli()
		if err := tx.Table("iteration_member_account").Create(map[string]interface{}{
			"member_code":       mem.Id,
			"organization_code": org.Id,
			"department_code":   0,
			"authorize":         strconv.FormatInt(defaultAuthId, 10),
			"is_owner":          1,
			"name":              mem.Name,
			"mobile":            mem.Mobile,
			"email":             mem.Email,
			"create_time":       now,
			"last_login_time":   now,
			"status":            1,
			"tenant_id":         tenant.IdFromContext(ctx),
		}).Error; err != nil {
			zap.L().Error("save member account err", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}

		evt := event.UserRegisterEvent{
			Uid:    mem.Id,
			Mobile: mem.Mobile,
			Email:  mem.Email,
			Name:   mem.Name,
		}
		body, _ := json.Marshal(evt)
		outbox := &outbox.Event{
			Topic:     evt.Topic(),
			MsgKey:    fmt.Sprintf("user:%d", mem.Id),
			Payload:   body,
			CreatedAt: time.Now(),
		}
		if err := ls.outboxRepo.Save(ctx, conn.(*gorms.GormConn).Tx(ctx), outbox); err != nil {
			zap.L().Error("save outbox err", zap.Error(err))
			return errs.GrpcError(model.DBError)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	_ = ls.cache.Del(ctx, captchaKey)

	return &login.RegisterResponse{}, nil
}

func (ls *LoginService) Login(c context.Context, msg *login.LoginMessage) (*login.LoginResponse, error) {
	lockKey := fmt.Sprintf("lock:login:%s", msg.Account)
	mutex := ls.redSync.NewMutex(
		lockKey,
		redsync.WithExpiry(5*time.Second),
		redsync.WithTries(1),
	)
	if err := mutex.LockContext(c); err != nil {
		zap.L().Warn("login frequent", zap.String("account", msg.Account), zap.String("ip", msg.Ip))
		return nil, errs.GrpcError(model.LoginFrequentError)
	}
	defer func() {
		_, _ = mutex.UnlockContext(c)
	}()

	var user *member.Member
	var orgs []*organization.Organization
	orgKey := fmt.Sprintf("user:org:%d:%s", tenant.IdFromContext(c), msg.Account)

	pwd := encrypts.HashPassword(msg.Password, msg.Account)
	lookupCtx := c
	if code := tenantCodeFromMetadata(c); code != "" {
		t, tErr := ls.tenantDao.FindByCode(c, code)
		if tErr != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		if t == nil || t.Status != 1 {
			return nil, errs.GrpcError(model.ParamsError)
		}
		lookupCtx = tenant.WithContext(c, tenant.Info{Id: t.Id, Code: t.Code})
	}
	mem, err := ls.memberRepo.FindMember(lookupCtx, msg.Account, pwd)
	if err != nil {
		zap.L().Error("find member error", zap.String("account", msg.Account), zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}
	if mem == nil {
		legacy := encrypts.Md5(msg.Password)
		mem, err = ls.memberRepo.FindMember(lookupCtx, msg.Account, legacy)
		if err != nil {
			zap.L().Error("find member error", zap.String("account", msg.Account), zap.Error(err))
			return nil, errs.GrpcError(model.DBError)
		}
		if mem != nil {
			if uErr := ls.memberRepo.UpdatePassword(lookupCtx, mem.Id, pwd); uErr != nil {
				zap.L().Warn("upgrade password hash failed",
					zap.String("account", msg.Account), zap.Error(uErr))
			} else {
				mem.Password = pwd
			}
		}
	}
	if mem == nil {
		zap.L().Warn("account or password error",
			zap.String("account", msg.Account), zap.String("ip", msg.Ip))
		return nil, errs.GrpcError(model.AccountAndPwdError)
	}
	user = mem

	memCtx := c
	if mem.TenantId > 0 {
		memCtx = tenant.WithContext(c, tenant.Info{Id: mem.TenantId, Code: tenant.DefaultCode})
		orgKey = fmt.Sprintf("user:org:%d:%s", mem.TenantId, msg.Account)
	}

	if ls.cache.Exist(memCtx, orgKey) {
		orgStr, _ := ls.cache.Get(memCtx, orgKey)
		_ = json.Unmarshal([]byte(orgStr), &orgs)
	}

	if orgs == nil {
		orgList, oErr := ls.organizationRepo.FindOrganizationByMemId(memCtx, mem.Id)
		if oErr != nil {
			return nil, errs.GrpcError(model.DBError)
		}
		orgs = orgList

		go func(os []*organization.Organization) {
			defer func() {
				if r := recover(); r != nil {
					zap.L().Error("cache panic", zap.Any("r", r))
				}
			}()
			oData, _ := json.Marshal(os)

			duration, _ := rand.Int(rand.Reader, big.NewInt(60))
			exp := 10*time.Minute + time.Duration(duration.Int64())*time.Second

			bgCtx := context.Background()
			_ = ls.cache.Put(bgCtx, orgKey, string(oData), exp)
		}(orgs)
	}

	memMsg := &login.MemberMessage{}
	_ = copier.Copy(memMsg, user)
	memMsg.Code, _ = encrypts.EncryptInt64(user.Id, model.AESKey)
	memMsg.LastLoginTime = tms.FormatByMilli(user.LastLoginTime)
	memMsg.CreateTime = tms.FormatByMilli(user.CreateTime)

	var orgMsgList []*login.OrganizationMessage
	for _, o := range orgs {
		code, _ := encrypts.EncryptInt64(o.Id, model.AESKey)
		orgMsgList = append(orgMsgList, &login.OrganizationMessage{
			Id:         o.Id,
			Name:       o.Name,
			Code:       code,
			OwnerCode:  memMsg.Code,
			CreateTime: tms.FormatByMilli(o.CreateTime),
		})
	}
	if len(orgMsgList) > 0 {
		memMsg.OrganizationCode, _ = encrypts.EncryptInt64(orgs[0].Id, model.AESKey)
	}

	memIdStr := fmt.Sprintf("%d", user.Id)
	exp := time.Duration(config.C.JwtConfig.AccessExp*3600*24) * time.Second
	rExp := time.Duration(config.C.JwtConfig.RefreshExp*3600*24) * time.Second

	tenantId := mem.TenantId
	if tenantId <= 0 {
		tenantId = tenant.DefaultId
	}
	if len(orgs) > 0 && orgs[0] != nil && orgs[0].TenantId > 0 {
		tenantId = orgs[0].TenantId
	}

	token := jwts.CreateTokenWithTenant(
		memIdStr,
		tenantId,
		exp,
		ls.accessPrivateKey,
		rExp,
		ls.refreshPrivateKey,
		msg.Ip,
	)
	if token == nil {
		zap.L().Error("CreateToken 返回 nil")
		return nil, errs.GrpcError(model.TokenGenerateError)
	}
	zap.L().Info("access token issued", zap.Int64("uid", user.Id), zap.Int64("exp", token.AccessExp))

	tokenList := &login.TokenMessage{
		AccessToken:    token.AccessToken,
		RefreshToken:   token.RefreshToken,
		AccessTokenExp: token.AccessExp,
		TokenType:      "bearer",
	}

	zap.L().Info("user login", zap.Int64("uid", user.Id), zap.String("ip", msg.Ip))
	return &login.LoginResponse{
		Member:           memMsg,
		OrganizationList: orgMsgList,
		TokenList:        tokenList,
	}, nil
}

func (ls *LoginService) TokenVerify(ctx context.Context, msg *login.LoginMessage) (*login.LoginResponse, error) {
	token := msg.Token
	token = strings.TrimPrefix(token, "bearer ")
	token = strings.TrimPrefix(token, "Bearer ")

	subject, _, err := jwts.ParseAccessTokenWithTenant(token, ls.publicKey, msg.Ip)
	if err != nil {
		zap.L().Error("parse token err", zap.Error(err))
		return nil, errs.GrpcError(model.NoLogin)
	}

	memberId, err := strconv.ParseInt(subject, 10, 64)
	if err != nil || memberId <= 0 {
		return nil, errs.GrpcError(model.NoLogin)
	}

	memberById, err := ls.memberRepo.FindMemberById(ctx, memberId)
	if err != nil || memberById == nil {
		return nil, errs.GrpcError(model.NoLogin)
	}

	memMsg := &login.MemberMessage{}
	_ = copier.Copy(memMsg, memberById)
	memMsg.Code = encrypts.EncryptNoErr(memberById.Id)
	memMsg.CreateTime = tms.FormatByMilli(memberById.CreateTime)
	memMsg.LastLoginTime = tms.FormatByMilli(memberById.LastLoginTime)

	orgs, _ := ls.organizationRepo.FindOrgListByMemberId(ctx, memberId)
	if len(orgs) > 0 && orgs[0] != nil {
		memMsg.OrganizationCode = encrypts.EncryptNoErr(orgs[0].Id)
	}

	return &login.LoginResponse{Member: memMsg}, nil
}

func (ls *LoginService) MyOrgList(ctx context.Context, msg *login.UserMessage) (*login.OrgListResponse, error) {
	memId := msg.MemId
	cacheKey := fmt.Sprintf("member:org:list:%d", memId)

	if ls.cache.Exist(ctx, cacheKey) {
		data, _ := ls.cache.Get(ctx, cacheKey)
		var list []*login.OrganizationMessage
		if json.Unmarshal([]byte(data), &list) == nil {
			return &login.OrgListResponse{OrganizationList: list}, nil
		}
	}

	orgs, err := ls.organizationRepo.FindOrgListByMemberId(ctx, memId)
	if err != nil {
		zap.L().Error("find org list err", zap.Int64("uid", memId), zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	var list []*login.OrganizationMessage
	_ = copier.Copy(&list, orgs)
	for _, item := range list {
		item.Code, _ = encrypts.EncryptInt64(item.Id, model.AESKey)
	}

	go func() {
		c := context.Background()

		if len(list) == 0 {
			_ = ls.cache.Put(c, cacheKey, "[]", 5*time.Minute)
			return
		}

		data, _ := json.Marshal(list)
		maxJitter := big.NewInt(120)
		n, err := rand.Int(rand.Reader, maxJitter)
		if err != nil {
			n = big.NewInt(60)
		}
		jitter := time.Duration(n.Int64()) * time.Second
		exp := 10*time.Minute + jitter

		_ = ls.cache.Put(c, cacheKey, string(data), exp)
	}()

	return &login.OrgListResponse{OrganizationList: list}, nil
}

func (ls *LoginService) FindMemInfoById(ctx context.Context, msg *login.UserMessage) (*login.MemberMessage, error) {
	memId := msg.MemId
	cacheKey := fmt.Sprintf("member:info:%d:%d", tenant.IdFromContext(ctx), memId)

	if ls.cache.Exist(ctx, cacheKey) {
		data, _ := ls.cache.Get(ctx, cacheKey)
		var mem *login.MemberMessage
		if json.Unmarshal([]byte(data), &mem) == nil && mem != nil && mem.Id > 0 {
			return mem, nil
		}
	}

	mb, err := ls.memberRepo.FindMemberById(ctx, memId)
	if err != nil {
		zap.L().Error("find member by id err", zap.Error(err))
		return nil, errs.GrpcError(model.DBError)
	}

	memMsg := &login.MemberMessage{}
	_ = copier.Copy(memMsg, mb)
	memMsg.Code = encrypts.EncryptNoErr(mb.Id)
	memMsg.CreateTime = tms.FormatByMilli(mb.CreateTime)

	orgs, _ := ls.organizationRepo.FindOrgListByMemberId(ctx, mb.Id)
	if len(orgs) > 0 {
		memMsg.OrganizationCode = encrypts.EncryptNoErr(orgs[0].Id)
	}

	go func(c context.Context) {
		defer func() {
			if r := recover(); r != nil {
				zap.L().Error("member info cache panic", zap.Any("r", r))
			}
		}()
		data, _ := json.Marshal(memMsg)
		d, _ := rand.Int(rand.Reader, big.NewInt(60))
		exp := 5*time.Minute + time.Duration(d.Int64())*time.Second
		bg, cancel := context.WithTimeout(c, 2*time.Second)
		defer cancel()
		_ = ls.cache.Put(bg, cacheKey, string(data), exp)
	}(context.Background())

	return memMsg, nil
}

func (ls *LoginService) FindMemInfoByIds(ctx context.Context, msg *login.UserMessage) (*login.MemberMessageList, error) {
	if len(msg.MIds) == 0 {
		return &login.MemberMessageList{List: nil}, nil
	}

	idSet := make(map[int64]struct{}, len(msg.MIds))
	uniqueIds := make([]int64, 0, len(msg.MIds))
	for _, id := range msg.MIds {
		if id <= 0 {
			continue
		}
		if _, ok := idSet[id]; ok {
			continue
		}
		idSet[id] = struct{}{}
		uniqueIds = append(uniqueIds, id)
	}
	if len(uniqueIds) == 0 {
		return &login.MemberMessageList{List: nil}, nil
	}
	memMsgs := make([]*login.MemberMessage, 0, len(uniqueIds))
	missIds := make([]int64, 0, len(uniqueIds))

	cacheKeys := make([]string, 0, len(uniqueIds))
	for _, id := range uniqueIds {
		cacheKeys = append(cacheKeys, fmt.Sprintf("member:info:%d:%d", tenant.IdFromContext(ctx), id))
	}
	hitMap, err := ls.cache.GetMulti(ctx, cacheKeys)
	if err != nil {

		zap.L().Warn("FindMemInfoByIds batch cache get failed, fallback to db", zap.Error(err))
		hitMap = map[string]string{}
	}

	for _, id := range uniqueIds {
		raw, ok := hitMap[fmt.Sprintf("member:info:%d", id)]
		if !ok {
			missIds = append(missIds, id)
			continue
		}
		var cached *login.MemberMessage
		if err := json.Unmarshal([]byte(raw), &cached); err != nil || cached == nil {

			missIds = append(missIds, id)
			continue
		}
		memMsgs = append(memMsgs, cached)
	}

	if len(missIds) == 0 {
		zap.L().Info("FindMemInfoByIds all hit cache", zap.Int("total", len(uniqueIds)))
		return &login.MemberMessageList{List: memMsgs}, nil
	}

	memberList, err := ls.memberRepo.FindMemberByIds(ctx, missIds)
	if err != nil {
		zap.L().Error("FindMemInfoByIds error", zap.Any("missIds", missIds), zap.Error(err))

		if len(memMsgs) > 0 {
			return &login.MemberMessageList{List: memMsgs}, nil
		}
		return nil, errs.GrpcError(model.DBError)
	}

	toCache := make(map[string]string, len(memberList))
	for _, mb := range memberList {
		if mb == nil {
			continue
		}
		memMsg := &login.MemberMessage{}
		_ = copier.Copy(memMsg, mb)
		memMsg.CreateTime = tms.FormatByMilli(mb.CreateTime)
		memMsg.Code = encrypts.EncryptNoErr(mb.Id)

		memMsgs = append(memMsgs, memMsg)

		if data, mErr := json.Marshal(memMsg); mErr == nil {
			toCache[fmt.Sprintf("member:info:%d", mb.Id)] = string(data)
		}
	}

	if len(toCache) > 0 {
		ls.batchBackfillMemberInfo(toCache)
	}

	return &login.MemberMessageList{List: memMsgs}, nil
}

func (ls *LoginService) batchBackfillMemberInfo(items map[string]string) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				zap.L().Error("batchBackfillMemberInfo panic", zap.Any("r", r))
			}
		}()

		d, _ := rand.Int(rand.Reader, big.NewInt(60))
		exp := 5*time.Minute + time.Duration(d.Int64())*time.Second

		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if err := ls.cache.PutMultiMemberAndOrg(bgCtx, items, exp, nil, 0); err != nil {
			zap.L().Error("batchBackfillMemberInfo put failed", zap.Error(err))
		}
	}()
}
