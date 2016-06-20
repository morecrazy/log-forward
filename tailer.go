package main

import (
	"github.com/hpcloud/tail"
	"backend/common"
	"math/rand"
	"strings"
	"time"
	"os"
)

func newTailer(fileName string) {
	group := strings.Split(fileName, ".")[0]
	brokers := common.Config.External[group]
	if brokers == "" {
		brokers = common.Config.External["other"]
	}
	brokerUrlList := StripRedisUrl(brokers)
	common.Logger.Info("Creating a new tailer for file %s", fileName)
	filePath := gFolderPath + fileName
	//针对每一个文件都创建一个tailer
	go tailer(filePath, brokerUrlList)
}

func tailer(fileName string, brokerList []string) {
	timer := time.NewTicker(time.Duration(gLogCleanTimeInterval) * time.Hour)
	logBufferList := []*LogBuffer{}
	mod := len(brokerList)
	//针对每一个broker都创建一个logBuffer
	for i := 0; i < mod; i++ {
		logBufferList = append(logBufferList,newLogBuffer(fileName, brokerList[i]))
	}
	//whence为0表示相对于文件的开始处，1表示相对于当前的位置，2表示相对于文件结尾
	seek := &tail.SeekInfo{Offset:0, Whence:2}
	t, _ := tail.TailFile(fileName, tail.Config{Location: seek, Follow: true})

	for line := range t.Lines {
		select {
		case <-timer.C:
			//清空文件
			pFile, _ := os.OpenFile(fileName, os.O_TRUNC, 0666)
			pFile.Close()
			//从开始位置读取
			seek = &tail.SeekInfo{Offset:0, Whence:0}
			t, _ = tail.TailFile(fileName, tail.Config{Location: seek, Follow: true})
		default:
			index := rand.Intn(100) % mod
			//fmt.Println(line.Text)
			//随机获取一个broker buffer进行写入操作
			logBuffer := logBufferList[index]
			if _, err := logBuffer.WriteString(line.Text); err != nil {
				common.Logger.Error(err.Error())
			}
		}
	}
}
