package main

import (
	"strings"
	"third/redigo/redis"
	"backend/common"
	"encoding/json"
)
type RedisBroker struct {
}

func (redisBroker *RedisBroker) GetBrokerList() ([]string, error) {
	redisUrlList := []string{}

	redisList := strings.Split(gBrokers, "|")
	for i := 0; i < len(redisList); i++ {
		redisPath := strings.Split(redisList[i], ":")
		redisHost := redisPath[0]
		redisPortList := strings.Split(redisPath[1], ",")
		for j := 0; j < len(redisPortList); j++ {
			redisUrl := redisHost + ":" + redisPortList[j]
			redisUrlList = append(redisUrlList, redisUrl)
		}
	}
	return redisUrlList, nil
}

func (redisBroker *RedisBroker) ProduceMsg(broker, topic, name, msg string) error  {
	c, err := redis.Dial("tcp", broker)
	if err != nil {
		common.Logger.Error("Connect to redis error: ", err)
		return err
	}
	defer c.Close()

	data := &Data{Path:name, Message:msg}
	b, err := json.Marshal(data)
	if err != nil {
		common.Logger.Error("json marshal msg failed: ", err)
		return err
	}
	_, err = redis.Int64(c.Do("LPUSH", topic, string(b)))
	if err != nil {
		common.Logger.Error("Redis write failed: ", err)
		return err
	}
	return nil
}
