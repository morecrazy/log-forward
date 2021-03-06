package main

import (
	"bytes"
	"sync"
	"backend/common"
	"os"
	"strings"
)
type LogBuffer struct {
	m *sync.Mutex
	buf *bytes.Buffer
	len int64
	linePrefix string
	linePostfix string
	ch chan bool
	brokeraddr string
	topic string
	name string
}

func (b *LogBuffer) WriteString(s string) (n int, err error) {
	b.m.Lock()
	//common.Logger.Debug("start write string to logbuffer, the log buffer name is %s, and the length is %d", b.broker, b.len)
	b.len ++
	b.m.Unlock()
	if b.len == gLogBufferSize {
		b.ch <- true
	}
	str := []string{}
	str = append(str, b.linePrefix)
	str = append(str, s)
	str = append(str, b.linePostfix)
	line := strings.Join(str, "")
	b.linePrefix = "\n"
	return b.buf.WriteString(line)
}

func (b *LogBuffer) ReadString() string {
	str := b.buf.String()
	//common.Logger.Debug("start read string from logbuffer, the log name is %s, the broker is %s ,and the length is %d", b.name, b.brokeraddr, b.len)
	b.buf.Reset()
	b.m.Lock()
	b.len = 0
	b.linePrefix = ""
	b.m.Unlock()
	return str
}

func newLogBuffer(fileName , topic string, broker Broker, brokeraddr string) *LogBuffer {
	common.Logger.Info("Creating a new logbuffer")
	logBuffer := new(LogBuffer)
	logBuffer.brokeraddr = brokeraddr
	logBuffer.topic = topic
	logBuffer.name = fileName
	logBuffer.buf = new(bytes.Buffer)
	logBuffer.m = new(sync.Mutex)
	logBuffer.ch = make(chan bool, 1)
	logBuffer.len = 0
	logBuffer.linePrefix = ""
	linePostfix, _ := os.Hostname()
	logBuffer.linePostfix = " [hostname:" + linePostfix + "]"
	//每创建一个logbuffer,同事创建一个forwarder读取buffer数据
	go forwarder(logBuffer, broker)
	return logBuffer
}