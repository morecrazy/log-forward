package main

import (
	"strings"
	"third/kafka"
	"backend/common"
	"encoding/json"
)

type KafkaBroker struct {
}

func (kafkaBroker *KafkaBroker) GetBrokerList() ([]string, error) {
	brokerList := strings.Split(gBrokers, "|")
	return brokerList, nil
}

func (kafkaBroker *KafkaBroker) ProduceMsg(broker, topic, name, msg string) error {
	config := kafka.NewConfig()
	config.Producer.RequiredAcks = kafka.WaitForAll
	config.Producer.Retry.Max = 5

	// brokers := []string{"192.168.59.103:9092"}
	brokers := []string{}
	brokers = append(brokers, broker)
	producer, err := kafka.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		common.Logger.Error(err.Error())
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			common.Logger.Error(err.Error())
		}
	}()

	content := &Data{Path:name, Message:msg}
	b, err := json.Marshal(content)
	data := &kafka.ProducerMessage{
		Topic: topic,
		//Partition: 0,
		Value: kafka.StringEncoder(b),
	}

	partition, offset, err := producer.SendMessage(data)
	if err != nil {
		common.Logger.Error(err.Error())
	}
	common.Logger.Info("The Message of Log file (%s) is stored in topic(%s)/partition(%d)/offset(%d)\n", name, topic, partition, offset)
	return nil
}

