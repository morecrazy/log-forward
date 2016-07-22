package main

type Broker interface {
	GetBrokerList() ([]string, error)
	ProduceMsg(broker, topic, name, msg string) error
}