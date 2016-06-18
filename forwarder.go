package main

import (
	"time"
	"third/redigo/redis"
	"backend/common"
	"encoding/json"
)

type Data struct {
	Path string `json:"path"`
	Message string `json:"message"`
}

func writeRedis(redisUrl , path, msg string) error {
	c, err := redis.Dial("tcp", redisUrl)
	if err != nil {
		common.Logger.Error("Connect to redis error: ", err)
		return err
	}
	defer c.Close()

	data := &Data{Path:path, Message:msg}
	b, err := json.Marshal(data)
	if err != nil {
		common.Logger.Error("json marshal msg failed: ", err)
		return err
	}
	_, err = redis.Int64(c.Do("LPUSH", gRedisKey, string(b)))
	if err != nil {
		common.Logger.Error("Redis write failed: ", err)
		return err
	}
	return nil
}

func forwarder(logBuffer *LogBuffer) {
	timer := time.NewTicker(1 * time.Second)
	for {
		select {
		case <- logBuffer.ch:
			//从buf读取数据,写入到redis
			msg := logBuffer.ReadString()
		 	if msg == "" {continue}
			if err := writeRedis(logBuffer.broker, logBuffer.name, msg); err != nil {
				common.Logger.Error(err.Error())
			}
		case <-timer.C:
			//超时时间到,强制读取数据
			//从buf读取数据,写入到redis
			msg := logBuffer.ReadString()
			if msg == "" {continue}
 			if err := writeRedis(logBuffer.broker, logBuffer.name, msg); err != nil {
				common.Logger.Error(err.Error())
			}
		}
	}
}
