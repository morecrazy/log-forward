package main

import (
	"time"
	"backend/common"
)

type Data struct {
	Path string `json:"path"`
	Message string `json:"message"`
}

func forwarder(logBuffer *LogBuffer, broker Broker) {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <- logBuffer.ch:
			//从buf读取数据,写入到broker
			msg := logBuffer.ReadString()
		 	if msg == "" {continue}
			if err := broker.ProduceMsg(logBuffer.brokeraddr, logBuffer.topic, logBuffer.name, msg); err != nil {
				common.Logger.Error(err.Error())
			}
		case <-timer.C:
			//超时时间到,强制读取数据
			//从buf读取数据,写入到broker
			msg := logBuffer.ReadString()
			if msg == "" {continue}
 			if err := broker.ProduceMsg(logBuffer.brokeraddr, logBuffer.topic, logBuffer.name, msg); err != nil {
				common.Logger.Error(err.Error())
			}
		}
	}
}
