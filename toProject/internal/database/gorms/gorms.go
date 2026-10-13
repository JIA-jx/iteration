package gorms

import (
	"context"
	"fmt"
	"sync"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"test.com/others/tenantdb"
	"test.com/toUser/config"
)

var (
	masterDB *gorm.DB

	slaveMu     sync.Mutex
	slaveDB     *gorm.DB
	slaveDSN    string
	slaveInited bool
)

func init() {
	masterDsn := buildDSN(
		config.C.MysqlConfig.Username,
		config.C.MysqlConfig.Password,
		config.C.MysqlConfig.Host,
		config.C.MysqlConfig.Port,
		config.C.MysqlConfig.Db,
	)
	var err error
	masterDB, err = gorm.Open(mysql.Open(masterDsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic("连接主库失败: " + err.Error())
	}
	if err := tenantdb.Register(masterDB); err != nil {
		panic("注册租户隔离插件失败: " + err.Error())
	}
}

func ConfigureSlave(dsn string) {
	if dsn == "" {
		return
	}

	slaveMu.Lock()
	defer slaveMu.Unlock()

	if slaveInited && dsn == slaveDSN {
		return
	}

	db, err := open(dsn)
	if err != nil {
		panic("连接从库失败: " + err.Error())
	}

	slaveDSN = dsn
	slaveDB = db
	slaveInited = true
}

func open(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return nil, err
	}
	if err := tenantdb.Register(db); err != nil {
		return nil, err
	}
	return db, nil
}

func slave() *gorm.DB {
	slaveMu.Lock()
	if slaveInited && slaveDB != nil {
		db := slaveDB
		slaveMu.Unlock()
		return db
	}

	dsn := BuildDSNFromMysqlConfig()
	if slaveDSN == dsn {
		slaveInited = true
	}
	slaveMu.Unlock()

	ConfigureSlave(dsn)

	slaveMu.Lock()
	defer slaveMu.Unlock()
	if slaveDB == nil {
		return masterDB
	}
	return slaveDB
}

func BuildDSNFromMysqlConfig() string {
	return buildDSN(
		config.C.MysqlConfig.Username,
		config.C.MysqlConfig.Password,
		config.C.MysqlConfig.Host,
		config.C.MysqlConfig.Port,
		config.C.MysqlConfig.Db,
	)
}

func GetDSN(username, password, host string, port int, dbName string) string {
	return buildDSN(username, password, host, port, dbName)
}

func buildDSN(username, password, host string, port int, dbName string) string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		username, password, host, port, dbName,
	)
}

func GetDB() *gorm.DB {
	return masterDB
}

type GormConn struct {
	db *gorm.DB
	tx *gorm.DB
}

func (g *GormConn) Begin() {
	g.tx = GetDB().Begin()
}

func New() *GormConn {
	return &GormConn{db: GetDB()}
}

func NewTran() *GormConn {
	return &GormConn{db: GetDB(), tx: GetDB()}
}

func (g *GormConn) Session(ctx context.Context) *gorm.DB {
	return g.db.Session(&gorm.Session{Context: ctx})
}

func (g *GormConn) Rollback() {
	g.tx.Rollback()
}
func (g *GormConn) Commit() error {
	return g.tx.Commit().Error
}

func (g *GormConn) Tx(ctx context.Context) *gorm.DB {
	return g.tx.WithContext(ctx)
}

func (g *GormConn) Slave() *gorm.DB {
	return slave()
}

func (g *GormConn) Reader(ctx context.Context) *gorm.DB {
	return Reader(ctx)
}

func (g *GormConn) Master() *gorm.DB {
	return masterDB
}
