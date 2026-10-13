package dao

import (
	"time"

	"test.com/others/idem"
	"test.com/toProject/internal/database/gorms"
)

var Idem *idem.Guard

func InitIdem(ttl time.Duration) *idem.Guard {
	var fast idem.Store
	if Rc != nil && Rc.Rdb != nil {
		fast = idem.NewRedisStore(Rc.Rdb)
	}

	authority := idem.NewDBStore(gorms.GetDB())
	Idem = idem.NewGuard(idem.NewTwoLevelStore(fast, authority), ttl)
	return Idem
}

func IdemGuard() *idem.Guard {
	return Idem
}

func IdemTTL() time.Duration {
	if Idem == nil {
		return idem.DefaultTTL
	}
	return Idem.TTL()
}
