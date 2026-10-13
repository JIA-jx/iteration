package config

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"log"
)

type NacosClient struct {
	confClient config_client.IConfigClient
	group      string
}

func InitNacosClient() *NacosClient {
	bootConf := InitBootstrap()

	log.Printf("Nacos连接配置: Namespace=%s, Group=%s, Addr=%s:%d, ContextPath=%s",
		bootConf.NacosConfig.Namespace,
		bootConf.NacosConfig.Group,
		bootConf.NacosConfig.IpAddr,
		bootConf.NacosConfig.Port,
		bootConf.NacosConfig.ContextPath,
	)

	clientConfig := constant.ClientConfig{
		NamespaceId:         bootConf.NacosConfig.Namespace,
		TimeoutMs:           500000000,
		NotLoadCacheAtStart: true,
		LogDir:              "D:/ProjectLogs/tmp/nacos/log",
		CacheDir:            "D:/ProjectLogs/tmp/nacos/cache",
		LogLevel:            "debug",
	}

	serverConfigs := []constant.ServerConfig{
		{
			IpAddr:      bootConf.NacosConfig.IpAddr,
			ContextPath: bootConf.NacosConfig.ContextPath,
			Port:        uint64(bootConf.NacosConfig.Port),
			Scheme:      bootConf.NacosConfig.Scheme,
		},
	}

	log.Printf("尝试连接Nacos服务器: %s://%s:%d%s",
		bootConf.NacosConfig.Scheme,
		bootConf.NacosConfig.IpAddr,
		bootConf.NacosConfig.Port,
		bootConf.NacosConfig.ContextPath,
	)

	configClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		log.Println("nacos client unavailable, fallback to local config:", err)
		return nil
	}
	nc := &NacosClient{
		confClient: configClient,
		group:      bootConf.NacosConfig.Group,
	}
	return nc
}
