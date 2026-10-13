package config

import (
	"github.com/spf13/viper"
	"log"
	"os"
)

type BootConf struct {
	viper       *viper.Viper
	NacosConfig *NacosConfig
}

func BootstrapAvailable() bool {
	workDir, _ := os.Getwd()
	candidates := []string{
		workDir + "/config/bootstrap.yaml",
		workDir + "/config/bootstrap.yml",
	}
	if dir := os.Getenv("APP_CONFIG_DIR"); dir != "" {
		candidates = append(candidates, dir+"/bootstrap.yaml", dir+"/bootstrap.yml")
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

func InitBootstrap() *BootConf {
	conf := &BootConf{viper: viper.New()}
	workDir, _ := os.Getwd()
	conf.viper.SetConfigName("bootstrap")
	conf.viper.SetConfigType("yaml")
	conf.viper.AddConfigPath(workDir + "/config")
	if dir := os.Getenv("APP_CONFIG_DIR"); dir != "" {
		conf.viper.AddConfigPath(dir)
	}
	err := conf.viper.ReadInConfig()
	if err != nil {
		log.Println("bootstrap config not found, use local defaults:", err)
		conf.NacosConfig = &NacosConfig{}
		return conf
	}
	conf.ReadNacosConfig()
	return conf
}

type NacosConfig struct {
	Namespace   string `yaml:"namespace"`
	Group       string `yaml:"group"`
	IpAddr      string `yaml:"ipAddr"`
	Port        int    `yaml:"port"`
	ContextPath string `yaml:"contextPath"`
	Scheme      string `yaml:"scheme"`
}

func (c *BootConf) ReadNacosConfig() {
	nc := &NacosConfig{}
	c.viper.UnmarshalKey("nacos", nc)

	c.NacosConfig = nc
}
