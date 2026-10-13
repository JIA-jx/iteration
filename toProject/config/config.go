package config

import (
	"bytes"
	"log"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/spf13/viper"
	"test.com/others/logs"
)

var (
	AIAPIKEY = C.Viper.GetString("set.ai_api_key")
)

var C = InitConfig()

type Config struct {
	Viper        *viper.Viper
	SC           *ServerConfig
	GC           *GrpcConfig
	EtcdConfig   *EtcdConfig
	MysqlConfig  *MysqlConfig
	JwtConfig    *JwtConfig
	DbConfig     DbConfig
	JaegerConfig *JaegerConfig
	KafkaConfig  *KafkaConfig
	DelayConfig  *DelayConfig
}

type DelayConfig struct {
	Shards    int
	OwnShards []int
}

type JaegerConfig struct {
	Endpoints string
}

type KafkaConfig struct {
	Brokers []string
}

func (c *Config) InitKafkaConfig() {
	var brokers []string
	if err := c.Viper.UnmarshalKey("kafka.brokers", &brokers); err != nil {
		log.Println("read kafka.brokers failed, use default:", err)
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	c.KafkaConfig = &KafkaConfig{Brokers: brokers}
}

type ServerConfig struct {
	Name          string
	Addr          string
	InternalToken string
	InternalAuth  bool
}

type GrpcConfig struct {
	Name     string
	EtcdAddr string
	Addr     string
	Version  string
	Weight   int64
}

type EtcdConfig struct {
	Addrs []string
}

type MysqlConfig struct {
	Username string
	Password string
	Host     string
	Port     int
	Db       string
	Name     string
}

type DbConfig struct {
	Master     MysqlConfig
	Slave      []MysqlConfig
	Separation bool
}

type JwtConfig struct {
	AccessExp     int64
	RefreshExp    int64
	AccessSecret  string
	RefreshSecret string
}

func InitConfig() *Config {
	conf := &Config{Viper: viper.New()}

	nacosClient := InitNacosClient()
	if nacosClient == nil || nacosClient.confClient == nil {
		log.Println("nacos unavailable, fallback to local config")
		conf.Viper.SetConfigType("yaml")
		workDir, _ := os.Getwd()
		conf.Viper.SetConfigName("config")
		conf.Viper.AddConfigPath(workDir + "/config")
		if err := conf.Viper.ReadInConfig(); err != nil {
			log.Println("local config file not found, use built-in defaults:", err)
		}
		conf.ReLoadAllConfig()
		return conf
	}
	configYaml, err2 := nacosClient.confClient.GetConfig(vo.ConfigParam{
		DataId: "config.yaml",
		Group:  nacosClient.group,
	})
	if err2 != nil {
		log.Println("nacos unavailable, fallback to local defaults:", err2)
		conf.ReLoadAllConfigQuiet()
		return conf
	}
	err2 = nacosClient.confClient.ListenConfig(vo.ConfigParam{
		DataId: "config.yaml",
		Group:  nacosClient.group,
		OnChange: func(namespace, group, dataId, data string) {
			log.Printf("load nacos config changed %s \n", data)
			err := conf.Viper.ReadConfig(bytes.NewBuffer([]byte(data)))
			if err != nil {
				log.Printf("load nacos config changed err : %s \n", err.Error())
			}
			conf.ReLoadAllConfig()
		},
	})
	if err2 != nil {
		log.Println("nacos listen config failed:", err2)
	}
	conf.Viper.SetConfigType("yaml")
	if configYaml != "" {
		if err := conf.Viper.ReadConfig(bytes.NewBuffer([]byte(configYaml))); err != nil {
			log.Println("parse nacos config failed:", err)
		}
	} else {
		workDir, _ := os.Getwd()
		conf.Viper.SetConfigName("config")
		conf.Viper.AddConfigPath(workDir + "/config")
		err := conf.Viper.ReadInConfig()
		if err != nil {
			log.Println("local config file not found, use built-in defaults:", err)
		}
	}
	conf.ReLoadAllConfig()
	return conf
}

func GetElkConfig() string {
	return C.Viper.GetString("elk.address")
}

func (c *Config) ReLoadAllConfig() {
	c.ReadServerConfig()
	c.InitZapLog()
	c.ReadGrpcConfig()
	c.ReadEtcdConfig()
	c.InitMysqlConfig()
	c.InitJwtConfig()
	c.InitDbConfig()
	c.InitJaegerConfig()
	c.InitKafkaConfig()
	c.InitDelayConfig()

	c.ReConnRedis()
}

func (c *Config) InitDelayConfig() {
	dc := &DelayConfig{
		Shards: c.Viper.GetInt("delay.shards"),
	}
	if dc.Shards <= 0 {
		dc.Shards = 1
	}

	var own []int
	if err := c.Viper.UnmarshalKey("delay.ownShards", &own); err == nil {
		dc.OwnShards = own
	}
	c.DelayConfig = dc
}
func (c *Config) InitJaegerConfig() {
	mc := &JaegerConfig{
		Endpoints: c.Viper.GetString("jaeger.endpoints"),
	}
	c.JaegerConfig = mc
}
func (c *Config) InitZapLog() {

	lc := &logs.LogConfig{
		DebugFileName: c.Viper.GetString("zap.debugFileName"),
		InfoFileName:  c.Viper.GetString("zap.infoFileName"),
		WarnFileName:  c.Viper.GetString("zap.warnFileName"),
		MaxSize:       c.Viper.GetInt("zap.maxSize"),
		MaxAge:        c.Viper.GetInt("zap.maxAge"),
		MaxBackups:    c.Viper.GetInt("zap.maxBackups"),
	}
	if err := logs.InitLogger(lc); err != nil {
		log.Println("init logger failed, continue without file logging:", err)
	}
}

func (c *Config) ReadServerConfig() {
	sc := &ServerConfig{}
	sc.Name = c.Viper.GetString("server.name")
	sc.Addr = c.Viper.GetString("server.addr")
	sc.InternalToken = c.Viper.GetString("server.internalToken")
	if token := os.Getenv("INTERNAL_API_TOKEN"); token != "" {
		sc.InternalToken = token
	}
	sc.InternalAuth = c.Viper.GetBool("server.internalAuth")
	if v := os.Getenv("INTERNAL_AUTH_ENABLED"); v != "" {
		sc.InternalAuth = v == "1" || v == "true"
	}
	c.SC = sc
}

func (c *Config) ReadGrpcConfig() {
	gc := &GrpcConfig{}
	gc.Name = c.Viper.GetString("grpc.name")
	gc.Addr = c.Viper.GetString("grpc.addr")
	gc.Version = c.Viper.GetString("grpc.version")
	gc.Weight = c.Viper.GetInt64("grpc.weight")
	gc.EtcdAddr = c.Viper.GetString("grpc.etcdAddr")
	c.GC = gc
}

func (c *Config) ReadRedisConfig() *redis.Options {
	return &redis.Options{
		Addr:     c.Viper.GetString("redis.host") + ":" + c.Viper.GetString("redis.port"),
		Password: c.Viper.GetString("redis.password"),
		DB:       c.Viper.GetInt("redis.db"),
	}
}

func (c *Config) ReadEtcdConfig() {
	ec := &EtcdConfig{}
	var addrs []string
	err := c.Viper.UnmarshalKey("etcd.addrs", &addrs)
	if err != nil {
		log.Printf("read etcd.addrs failed, keep previous value: %v", err)
		if c.EtcdConfig != nil {
			return
		}
	} else {
		ec.Addrs = addrs
	}
	c.EtcdConfig = ec
}
func (c *Config) InitMysqlConfig() {
	mc := &MysqlConfig{
		Username: c.Viper.GetString("mysql.username"),
		Password: c.Viper.GetString("mysql.password"),
		Host:     c.Viper.GetString("mysql.host"),
		Port:     c.Viper.GetInt("mysql.port"),
		Db:       c.Viper.GetString("mysql.db"),
	}
	c.MysqlConfig = mc
}
func (c *Config) InitJwtConfig() {
	mc := &JwtConfig{
		AccessSecret:  c.Viper.GetString("jwt.accessSecret"),
		AccessExp:     c.Viper.GetInt64("jwt.accessExp"),
		RefreshExp:    c.Viper.GetInt64("jwt.refreshExp"),
		RefreshSecret: c.Viper.GetString("jwt.refreshSecret"),
	}
	c.JwtConfig = mc
}

func (c *Config) InitDbConfig() {
	mc := DbConfig{}
	mc.Separation = c.Viper.GetBool("db.separation")
	var slaves []MysqlConfig
	err := c.Viper.UnmarshalKey("db.slave", &slaves)
	if err != nil {
		panic(err)
	}
	master := MysqlConfig{
		Username: c.Viper.GetString("db.master.username"),
		Password: c.Viper.GetString("db.master.password"),
		Host:     c.Viper.GetString("db.master.host"),
		Port:     c.Viper.GetInt("db.master.port"),
		Db:       c.Viper.GetString("db.master.db"),
	}
	mc.Master = master
	mc.Slave = slaves
	c.DbConfig = mc
}
func (c *Config) ReLoadAllConfigQuiet() {
	c.ReadServerConfig()
	c.ReadGrpcConfig()
	c.ReadEtcdConfig()
	c.InitMysqlConfig()
	c.InitJwtConfig()
	c.InitDbConfig()
	c.InitJaegerConfig()
	c.InitKafkaConfig()
	c.InitDelayConfig()
}
