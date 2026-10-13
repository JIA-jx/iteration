package config

import (
	"log"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"test.com/others/logs"
)

var C = InitConfig()

type Config struct {
	SmsConfig    *SmsConfig
	viper        *viper.Viper
	SC           *ServerConfig
	GC           *GrpcConfig
	EtcdConfig   *EtcdConfig
	MysqlConfig  *MysqlConfig
	JwtConfig    *JwtConfig
	JaegerConfig *JaegerConfig
	KafkaConfig  *KafkaConfig
}

type JaegerConfig struct {
	Endpoints string
}

type KafkaConfig struct {
	Brokers []string
}

func (c *Config) InitKafkaConfig() {
	var brokers []string
	if err := c.viper.UnmarshalKey("kafka.brokers", &brokers); err != nil {
		log.Fatalln(err)
	}
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	c.KafkaConfig = &KafkaConfig{Brokers: brokers}
}

type ServerConfig struct {
	Name string
	Addr string
}

type GrpcConfig struct {
	Name     string
	Addr     string
	Version  string
	Weight   int64
	EtcdAddr string
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
}

type JwtConfig struct {
	AccessExp     int64
	RefreshExp    int64
	AccessSecret  string
	RefreshSecret string
}

func InitConfig() *Config {
	conf := &Config{viper: viper.New()}

	conf.viper.SetConfigName("config")
	conf.viper.SetConfigType("yaml")
	conf.viper.AddConfigPath("./config")
	err := conf.viper.ReadInConfig()
	if err != nil {
		log.Fatalln(err)
	}

	conf.ReadServerConfig()
	conf.InitZapLog()
	conf.ReadGrpcConfig()
	conf.ReadEtcdConfig()
	conf.InitMysqlConfig()
	conf.InitJwtConfig()
	conf.InitJaegerConfig()
	conf.InitKafkaConfig()
	conf.InitSmsConfig()
	return conf
}

func (c *Config) InitZapLog() {
	lc := &logs.LogConfig{
		DebugFileName: c.viper.GetString("zap.debugFileName"),
		InfoFileName:  c.viper.GetString("zap.infoFileName"),
		WarnFileName:  c.viper.GetString("zap.warnFileName"),
		MaxSize:       c.viper.GetInt("zap.maxSize"),
		MaxAge:        c.viper.GetInt("zap.maxAge"),
		MaxBackups:    c.viper.GetInt("zap.maxBackups"),
	}
	err := logs.InitLogger(lc)
	if err != nil {
		log.Fatalln(err)
	}
}

func (c *Config) ReadServerConfig() {
	sc := &ServerConfig{}
	sc.Name = c.viper.GetString("server.name")
	sc.Addr = c.viper.GetString("server.addr")
	c.SC = sc
}

func (c *Config) ReadGrpcConfig() {
	gc := &GrpcConfig{}
	gc.Name = c.viper.GetString("grpc.name")
	gc.Addr = c.viper.GetString("grpc.addr")
	gc.Version = c.viper.GetString("grpc.version")
	gc.Weight = c.viper.GetInt64("grpc.weight")
	gc.EtcdAddr = c.viper.GetString("grpc.etcdAddr")
	c.GC = gc
}

func (c *Config) ReadRedisConfig() *redis.Options {
	return &redis.Options{
		Addr:     c.viper.GetString("redis.host") + ":" + c.viper.GetString("redis.port"),
		Password: c.viper.GetString("redis.password"),
		DB:       c.viper.GetInt("redis.db"),
	}
}

func (c *Config) ReadEtcdConfig() {
	ec := &EtcdConfig{}
	var addrs []string
	err := c.viper.UnmarshalKey("etcd.addrs", &addrs)
	if err != nil {
		log.Fatalln(err)
	}
	ec.Addrs = addrs
	c.EtcdConfig = ec
}

func (c *Config) InitMysqlConfig() {
	mc := &MysqlConfig{
		Username: c.viper.GetString("mysql.username"),
		Password: c.viper.GetString("mysql.password"),
		Host:     c.viper.GetString("mysql.host"),
		Port:     c.viper.GetInt("mysql.port"),
		Db:       c.viper.GetString("mysql.db"),
	}
	c.MysqlConfig = mc
}

func (c *Config) InitJwtConfig() {
	mc := &JwtConfig{
		AccessSecret:  c.viper.GetString("jwt.accessSecret"),
		AccessExp:     c.viper.GetInt64("jwt.accessExp"),
		RefreshExp:    c.viper.GetInt64("jwt.refreshExp"),
		RefreshSecret: c.viper.GetString("jwt.refreshSecret"),
	}
	c.JwtConfig = mc
}

func (c *Config) InitJaegerConfig() {
	mc := &JaegerConfig{
		Endpoints: c.viper.GetString("jaeger.endpoints"),
	}
	c.JaegerConfig = mc
}

type SmsConfig struct {
	EchoCode bool
}

func (c *Config) InitSmsConfig() {
	sc := &SmsConfig{EchoCode: c.viper.GetBool("sms.echoCode")}
	if v := os.Getenv("SMS_ECHO_CODE"); v != "" {
		sc.EchoCode = v == "1" || v == "true"
	}
	c.SmsConfig = sc
}
