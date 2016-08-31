package main

import (
	"github.com/hpcloud/tail"
	"backend/common"
	"math/rand"
	"strings"
)

func newTailer(fileName string, broker Broker) {
	group := strings.Split(fileName, ".")[0]
	topic := common.Config.External[group]
	if topic == "" {
		topic = common.Config.External["other"]
	}
	common.Logger.Info("Creating a new tailer for file %s", fileName)
	filePath := gFolderPath + fileName

	brokerList,_ := broker.GetBrokerList()
	//针对每一个文件都创建一个tailer
	go tailer(filePath, topic, broker, brokerList)
}

func tailer(fileName, topic string, broker Broker, brokerList []string) {
	logBufferList := []*LogBuffer{}
	mod := len(brokerList)
	//针对每一个broker都创建一个logBuffer
	for i := 0; i < mod; i++ {
		logBufferList = append(logBufferList,newLogBuffer(fileName, topic, broker, brokerList[i]))
	}
	//whence为0表示相对于文件的开始处，1表示相对于当前的位置，2表示相对于文件结尾
	seek := &tail.SeekInfo{Offset:0, Whence:2}
	t, _ := tail.TailFile(fileName, tail.Config{Location: seek, Follow: true})


	for {
		select {
		case line := <- t.Lines:
			index := rand.Intn(100) % mod
			if fileName == "/var/log/go_log/ucenter.log" {
				common.Logger.Info("Read from file %s", line.Text)
			}
			//随机获取一个broker buffer进行写入操作
			logBuffer := logBufferList[index]
			if _, err := logBuffer.WriteString(line.Text); err != nil {
				common.Logger.Error(err.Error())
			}
		}
	}
}
