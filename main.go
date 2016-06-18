package main

import (
	"fmt"
	"backend/common"
	"flag"
	"runtime"
	"sync"
	"strings"
	"codoon_ops/log-forward/util/set"
	"time"

)

var wg sync.WaitGroup

const (
	DEFAULT_CONF_FILE = "./log-sink.conf"
)

var g_conf_file string
var gRedisKey string
var gChannelBufferSize int64
var gBufferWriterNum int64
var gLogSize int64
var gLogUnit string
var gLogBufferSize int64
var gFolderPath string

func init() {
	const usage = "log-sink [-c config_file]"
	flag.StringVar(&g_conf_file, "c", "", usage)
}

func InitExternalConfig(config *common.Configure)  {
	gFolderPath = config.External["path"]
	gRedisKey = config.External["redisKey"]
	gLogUnit = config.External["logUnit"]
	gLogSize = config.ExternalInt64["logSize"]
	gChannelBufferSize = config.ExternalInt64["channelBufferSize"]
	gBufferWriterNum = config.ExternalInt64["bufferWriterNum"]
	gLogBufferSize = config.ExternalInt64["logBufferSize"]
}

func StripRedisUrl(redisPath string) []string {
	redisUrlList := []string{}

	redisList := strings.Split(redisPath, "|")
	for i := 0; i < len(redisList); i++ {
		redisPath := strings.Split(redisList[i], ":")
		redisHost := redisPath[0]
		redisPortList := strings.Split(redisPath[1], ",")
		for j := 0; j < len(redisPortList); j++ {
			redisUrl := redisHost + ":" + redisPortList[j]
			redisUrlList = append(redisUrlList, redisUrl)
		}
	}
	return redisUrlList
}

func checkNewFile(fileNameSet *set.Set) {
	common.Logger.Debug("Starting check file folder")
	fileNameList := GetFolderFileNames()
	for i := 0; i < len(fileNameList); i++ {
		filename := fileNameList[i]
		//如果文件集合里已经存在此文件了,则忽略
		if fileNameSet.Has(filename) {continue}
		//否则,新开一个tailer,并且将此文件加入文件集合中
		fileNameSet.Add(filename)
		newTailer(filename)
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
		newTailer(item)
	}

	fmt.Println("Sink log service is started...")
	timer := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-timer.C:
			checkNewFile(fileNameSet)
		}
	}
}

