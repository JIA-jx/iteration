package config

import (
	"errors"
	"github.com/spf13/viper"
	"log"
	"os"
	"test.com/others/logs"
)

var C = InitConfig()

type Config struct {
	Viper *viper.Viper
	SC    *ServerConfig
	GC    *GrpcConfig
	EC    *EtcdConfig
	JC    *JaegerConfig
	KC    *KafkaConfig
	MC    *MinioConfig
	SMSC  *SmsConfig
}

type MinioConfig struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Secure    bool
}

type SmsConfig struct {
	EchoCode bool
}

func (c *Config) InitMinioConfig() {
	mc := &MinioConfig{
		Endpoint:  c.Viper.GetString("minio.endpoint"),
		AccessKey: c.Viper.GetString("minio.accessKey"),
		SecretKey: c.Viper.GetString("minio.secretKey"),
		Bucket:    c.Viper.GetString("minio.bucket"),
		Secure:    c.Viper.GetBool("minio.secure"),
	}
	if v := os.Getenv("MINIO_ENDPOINT"); v != "" {
		mc.Endpoint = v
	}
	if v := os.Getenv("MINIO_ACCESS_KEY"); v != "" {
		mc.AccessKey = v
	}
	if v := os.Getenv("MINIO_SECRET_KEY"); v != "" {
		mc.SecretKey = v
	}
	if v := os.Getenv("MINIO_BUCKET"); v != "" {
		mc.Bucket = v
	}
	if mc.Bucket == "" {
		mc.Bucket = "iteration"
	}
	c.MC = mc
}

func (c *Config) InitSmsConfig() {
	sc := &SmsConfig{EchoCode: c.Viper.GetBool("sms.echoCode")}
	if v := os.Getenv("SMS_ECHO_CODE"); v != "" {
		sc.EchoCode = v == "1" || v == "true"
	}
	c.SMSC = sc
}

type KafkaConfig struct {
	Brokers []string
}

type JaegerConfig struct {
	Endpoints string
}

type EtcdConfig struct {
	Addrs []string
}

type ServerConfig struct {
	Name string
	Addr string
}

type GrpcConfig struct {
	Name string
	Addr string
}

func InitConfig() *Config {
	conf := &Config{Viper: viper.New()}

	conf.Viper.SetConfigName("config")
	conf.Viper.SetConfigType("yaml")
	conf.Viper.AddConfigPath("./config")
	err := conf.Viper.ReadInConfig()
	if err != nil {
		log.Fatalln(errors.New("api层配置读取失败"))
	}

	conf.InitZapLog()
	conf.InitServerConfig()
	conf.InitEtcdConfig()
	conf.InitJaegerConfig()
	conf.InitKafkaConfig()
	conf.InitMinioConfig()
	conf.InitSmsConfig()
	return conf
}

func (c *Config) InitKafkaConfig() {
	brokers := c.Viper.GetStringSlice("kafka.brokers")
	if len(brokers) == 0 {
		brokers = []string{"127.0.0.1:9092"}
	}
	c.KC = &KafkaConfig{Brokers: brokers}
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
	err := logs.InitLogger(lc)
	if err != nil {
		log.Fatalln(err)
	}
}

func (c *Config) InitServerConfig() {
	sc := &ServerConfig{}
	sc.Name = c.Viper.GetString("server.name")
	sc.Addr = c.Viper.GetString("server.addr")
	c.SC = sc
}

func (c *Config) InitEtcdConfig() {
	ec := &EtcdConfig{}
	var addrs []string
	err := c.Viper.UnmarshalKey("etcd.addrs", &addrs)
	if err != nil {
		log.Fatalln(err)
	}
	ec.Addrs = addrs
	c.EC = ec
}

func (c *Config) InitJaegerConfig() {
	mc := &JaegerConfig{
		Endpoints: c.Viper.GetString("jaeger.endpoints"),
	}
	c.JC = mc
}
