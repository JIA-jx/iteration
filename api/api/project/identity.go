package project

import (
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"test.com/api/api/rpc"
	"test.com/rpcOrgrpc/user/login"
)

const organizationCodeKey = "organizationCode"

const (
	orgCodeTTL     = 30 * time.Second
	orgCodeMaxSize = 20000
)

type orgCodeEntry struct {
	code    string
	expires time.Time
}

var (
	orgCodeMu    sync.RWMutex
	orgCodeCache = make(map[int64]orgCodeEntry)
)

func PurgeAllOrgCodeCache() {
	orgCodeMu.Lock()
	orgCodeCache = make(map[int64]orgCodeEntry)
	orgCodeMu.Unlock()
}

func OrganizationCode(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if v, ok := c.Get(organizationCodeKey); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}

	memberId := c.GetInt64("memberId")
	if memberId <= 0 {
		return ""
	}

	orgCodeMu.RLock()
	entry, ok := orgCodeCache[memberId]
	orgCodeMu.RUnlock()
	if ok && time.Now().Before(entry.expires) && entry.code != "" {
		c.Set(organizationCodeKey, entry.code)
		return entry.code
	}

	ctx := c.Request.Context()
	list, err := rpc.LoginServiceClient.MyOrgList(ctx, &login.UserMessage{MemId: memberId})
	if err != nil || list == nil {
		return ""
	}

	for _, org := range list.OrganizationList {
		if org == nil || org.Code == "" {
			continue
		}
		orgCodeMu.Lock()
		if len(orgCodeCache) >= orgCodeMaxSize {
			for k, v := range orgCodeCache {
				if time.Now().After(v.expires) {
					delete(orgCodeCache, k)
				}
			}
			if len(orgCodeCache) >= orgCodeMaxSize {
				orgCodeCache = make(map[int64]orgCodeEntry)
			}
		}
		orgCodeCache[memberId] = orgCodeEntry{code: org.Code, expires: time.Now().Add(orgCodeTTL)}
		orgCodeMu.Unlock()

		c.Set(organizationCodeKey, org.Code)
		return org.Code
	}

	return ""
}
