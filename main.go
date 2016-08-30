package main

import (
	"fmt"
	"backend/common"
	"flag"
	"runtime"
	"sync"
	"codoon_ops/log-forward/util/set"
	"time"

)

const (
	DEFAULT_CONF_FILE = "./log-sink.conf"
)

var (
	wg sync.WaitGroup
	g_conf_file string
	gRedisKey string

	gLogSize int64
	gLogUnit string
	gLogBufferSize int64
	gFolderPath string
	gBrokers string
)

func init() {
	const usage = "log-sink [-c config_file]"
	flag.StringVar(&g_conf_file, "c", "", usage)
}

func InitExternalConfig(config *common.Configure)  {
	gFolderPath = config.External["path"]
	gRedisKey = config.External["redisKey"]
	gLogUnit = config.External["logUnit"]
	gBrokers = config.External["brokers"]
	gLogSize = config.ExternalInt64["logSize"]

	gLogBufferSize = config.ExternalInt64["logBufferSize"]
}

func checkNewFile(fileNameSet *set.Set, broker Broker) {
	common.Logger.Info("Starting check file folder")
	fileNameList := GetFolderFileNames()
	for i := 0; i < len(fileNameList); i++ {
		filename := fileNameList[i]
		//如果文件集合里已经存在此文件了,则忽略
		if fileNameSet.Has(filename) {continue}
		//否则,新开一个tailer,并且将此文件加入文件集合中
		fileNameSet.Add(filename)
		newTailer(filename, broker)
	}
}

func main() {
	//set runtime variable
	runtime.GOMAXPROCS(runtime.NumCPU())
	//get flag
	flag.Parse()

	if g_conf_file != "" {
		common.Config = new(common.Configure)
		if err := common.InitConfigFile(g_conf_file, common.Config); err != nil {
			fmt.Println("init config err : ", err)
		}
	} else {
		addrs := []string{"http://etcd.in.codoon.com:2379"}
		common.Config = new(common.Configure)
		if err := common.LoadCfgFromEtcd(addrs, "log-forward", common.Config); err != nil {
			fmt.Println("init config from etcd err : ", err)
		}
	}

	var err error
	broker := new(KafkaBroker) //注入broker

	common.Logger, err = common.InitLogger("log-forward")
	if err != nil {
		fmt.Println("init log error")
		return
	}
	InitExternalConfig(common.Config)

	var fileNameSet = set.New()
	fileNameList := GetFolderFileNames()
	for _, item := range fileNameList {
		fileNameSet.Add(item)
		newTailer(item, broker)
	}

	fmt.Println("forward log service is started...")
	timer := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-timer.C:
			checkNewFile(fileNameSet, broker)
		}
	}
}

